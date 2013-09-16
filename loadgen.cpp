#include <iostream>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <poll.h>
#include <signal.h>

#include <unistd.h>
#include <time.h>

#include <errno.h>
#include <assert.h>


using namespace std;

///// GLOBALS /////

bool done = false;
const int READ_BUF_SIZE = 4096;
char requestBuf[]= "GET / HTTP/1.1\r\n\r\n";
int requestSize = strlen(requestBuf);
int nThreads = 0;


class Threads{

	public:
		Threads() {};
		~Threads() {};

		void Start(){

			int ret = pthread_create(&id,0, InitThread,this);
			if(ret !=0)
				throw "Thread creation error\n";
		}

		void Join(){
			pthread_join(id,0);
		}

	protected:
		pthread_t id;
		pthread_key_t key;

		static void * InitThread(void *t){
			((Threads *)t)->Start();
		}	
};

class Worker: public Threads{

	public:	
		Worker(){}
		~Worker(){};

		enum States{
			Closed =0,
			Sending,
			Receiving
		};

		enum CounterLabels{
			byteSent =0,
			receiveBytes,
			connections,
			sessions,
			incompleteWrite,
			writeError,
			writeEintr,
			readEagain,
			badConnect,
			closed,
			socketFail,
			badReceive
		};

		static struct sockaddr_in server;
		static const int NCOUNTER = 32;

		/*Data Members*/
		int id;
		char *ip;
		unsigned int port;
		int maxConn;
		int npackets;
		int nSessions;
		int sleep;
		int *socks;
		States *state;
		int *packetsSent;
		long counter[NCOUNTER];
		int *seq;

		int Init(char *ipStr, unsigned short prt,int mconn, int np, int sessions,int secs);
		int Run();

		int Connect(){
		
			int sock = socket(AF_INET,SOCK_STREAM,0);
			if(sock < 0){
				counter[socketFail] ++;
				return -1;
			}

			errno =0;
			int ret = connect(sock, (struct sockaddr *) &server, sizeof(server));
			
			if(ret < 0)
				return -1;

			return sock;
		}
		
		static void InitStatic(char *hostname,unsigned int port){
		
			struct hostent *hp = gethostbyname(hostname);
			memset(&server,0,sizeof(server));
			server.sin_family = AF_INET;
			bcopy( hp->h_addr, &(server.sin_addr.s_addr),hp->h_length);
			server.sin_port = htons(port);
		}

		int Start() {
			Run(); 
			Terminate();
		}

		void Terminate() {
			cout << "Worker shutdown " << id<<endl<<std::flush;
			for(int i=0; i< maxConn; i++){
				if(socks[i] != -1){
					shutdown(socks[i],SHUT_RDWR);
					close(socks[i]);
				}	
			}
		}

		/*IO*/
		int Write(int sock,const char* buf, int bytes);
		int Read(int sock);
		int Send(int threadIndex);
		int Recieve(int threadIndex);

	private:
		/*Utils*/
		int SetNonblocking(int fd){
			int flags;

			if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
				flags = 0;
			return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
		}

		int SetBlocking(int fd){
			int flags;

			if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
				flags = 0;
			return fcntl(fd, F_SETFL, flags & ~ (O_NONBLOCK));
		}

		/* Check if the socket is non blocking connected*/
		int CheckSocketNonBlocked(int sock){
			socklen_t len;
			int err = -1;
			len = sizeof(int);
			return getsockopt(sock,SOL_SOCKET,SO_ERROR,&err,&len);
		}
		

};

////// Class Methods ///////

int Worker::Init(char *ipStr, unsigned short prt,int mconn, int np, int sessions,int secs){

	ip = ipStr;
	port = prt;
	maxConn = mconn;
	npackets =np;
	nSessions = sessions;
	sleep = secs;

	socks = new int [ maxConn];
	state = new States[maxConn];

	for(int i=0; i<maxConn; i++){
		socks[i] = -1;
		state[i] = Closed;	
	}

	for(int i=0; i< NCOUNTER; i++)
			counter[i] = 0;
	

}


int Worker::Read(int sock){

	errno =0;
	int ret = -1;
	
	//Set blocking
	if( (ret = SetNonblocking(sock)) < 0)
		return -1;

	char buf[READ_BUF_SIZE];
	ret = read(sock,buf,READ_BUF_SIZE);

	if(ret  <=0 ){
	
		if(ret == 0)  //socket EOF
			return -1;
		
		if( errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN){
			// Didn't read all the bytes
			counter[readEagain] ++ ;
			return 0;
		}else{
			return -1;
		}

	}else{
		counter[receiveBytes] += ret;
		//Print Contents
	}

	// set blocking
	if( (ret = SetBlocking(sock)) < 0) {
		return -1;
	}
	return ret;	
}

int Worker::Write(int sock,const char *buf,int bytes){

	errno =0;
	int ret = write(sock,buf,bytes);
	
	if(ret == -1 || ret < bytes){
		if (errno == EINTR){
			counter[writeEintr] ++;
			return 0;
		}
		counter[writeError]++;
		return ret;
	}
	counter[byteSent] += ret;
	return ret;
}

int Worker::Send(int threadIndex){

	errno =0;
	int ret = Write(socks[threadIndex],requestBuf,requestSize);
       	if(ret != requestSize){
       		counter[incompleteWrite] ++;
		return -1;
	}
	return 0;
	
}

int Worker::Run(){
	
	while( counter[sessions] < nSessions && done == false){
	
		int i = rand() %maxConn;
		if( state[i] == Closed ){
			socks[i] = -1;
			socks[i] = Connect();

			//Connect
			if(socks[i] > 0){ //valid fd
			
				counter[connections] ++;

				int res = Send(i);
				if(res ==0){
					state[i] = Receiving;
					shutdown(socks[i],SHUT_WR);
				}
				else{
					close(socks[i]);
					socks[i] = -1;
					state[i] = Closed;
					counter[closed] ++;
					counter[connections]--;
				}
			}else{
				counter[badConnect] ++;
				socks[i] = -1;
				state[i] = Closed;
			}

		}
		
		//Receive
		else if(state[i] == Receiving){
			
			int res = Read(socks[i]);
			if (res >0){
			
				close(socks[i]);
				socks[i] = -1;
				state[i] = Closed;
				
				if (errno !=0)
					counter[badReceive] ++;

				counter[closed] ++;
				counter[connections] --;

			}else{
				// do nothing, no bytes read
			}
			
		}


	}

};

//////////// MAIN ////////////

struct sockaddr_in Worker::server;
Worker *workers =0;

void TrapSignal(int s){
	done  = true;
}

void TrapDeath(int s){
	cout << "SIGALRM \n" << endl;
	exit(1);
}

static long GetCounterStats(Worker::CounterLabels label){

	long res =0;
	for(int i=0; i < nThreads; i++){
		res  += workers[i].counter[label];
	}
	return res;
}

static int readFileContents(const char *fname, char * buf){

	struct stat s;
	int ret = stat(fname,&s);

	if(ret <0) 
		throw "read err";

	FILE *f = fopen(fname,"r");
	if(f == 0)
		throw "file open err";

	fread(buf,1,s.st_size,f);
	buf[s.st_size] = 0;
	fclose(f);

	return s.st_size;

}

int main(int argc, char **argv){

	if(argc <9){
		cout << "./loadgen nthreds host port maxConnections npackets nSessions sleepInterval filename printMsg" <<endl;
		exit(0);
	}

	//Signals
	signal(SIGINT,&TrapSignal);
	signal(SIGALRM,&TrapDeath);
	signal(SIGPIPE,SIG_IGN);
	signal(SIGCHLD,SIG_IGN);

	nThreads = atoi(argv[1]);
	char *host =argv[2];
	unsigned short port = atoi(argv[3]);
	int maxConn = atoi(argv[4]);
	int npackets = atoi(argv[5]);
	int sessions = atoi(argv[6]);
	int sleepInterval = atoi(argv[7]);
	char *filename = argv[8];
	int print = atoi(argv[9]);


	requestSize =	readFileContents(filename, requestBuf);
	
	Worker::InitStatic(host,port);
	workers = new Worker[nThreads];

	//Init threads
	for(int i =0; i <nThreads; i++){
		workers[i].Init(host,port,maxConn/nThreads,npackets,sessions,sleepInterval);	
	}

	//Start Threads

	for(int i =0; i <nThreads; i++){
		workers[i].Start();
		workers[i].id = i;
	}
	// Run Loop	

	time_t start = time(0);
	time_t prev = time(0);
	time_t lastPrint = time(0);

	int interval = 10; //10 secs
	int rps =0, prevRps = 0;
	int dataRate = 0, prevDataRate =0;

	while(done == false){

		time_t t = time(0);

		// Calculate request rate
		if( t >= (prev + interval)){
		
			prev = t;
			long cur = GetCounterStats(Worker::byteSent);
			dataRate  =  (cur - prevDataRate) / interval ;			
			prevDataRate = dataRate;

			rps = (GetCounterStats(Worker::closed) - prevRps) / interval ;
			prevRps = GetCounterStats(Worker::closed);

		}

		if(t > lastPrint){
			cout <<endl<< "Open Connections" << GetCounterStats(Worker::connections);	
			cout <<endl<< "RPS (Moving Average)" << GetCounterStats(Worker::connections) / difftime(time(0),start);
			cout <<endl<< "RPS Overall" << rps;
			cout <<endl<< "Connections closed so far" << GetCounterStats(Worker::closed);
		
			cout <<endl<<std::flush;	
			lastPrint = t;	
		}

		sleep(1);
	}


	// join all the threads	
	for(int i =0; i <nThreads; i++){
		workers[i].Join();
	}
		
}

