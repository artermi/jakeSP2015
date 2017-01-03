#include "csiebox_server.h"

#include "csiebox_common.h"
#include "connect.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <utime.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>

static int thread_working(void* data);
static int parse_arg(csiebox_server* server, int argc, char** argv);
static int handle_request(csiebox_server* server, int conn_fd, int fdmax);
static int get_account_info(csiebox_server* server,  const char* user, csiebox_account_info* info);
static void login(csiebox_server* server, int conn_fd, csiebox_protocol_login* login);
static void logout(csiebox_server* server, int conn_fd);
static void sync_file(csiebox_server* server, int conn_fd, csiebox_protocol_meta* meta, int fdmax);
static char* get_user_homedir(csiebox_server* server, csiebox_client_info* info);
static void rm_file(csiebox_server* server, int conn_fd, csiebox_protocol_rm* rm, int fdmax);

void csiebox_server_init(csiebox_server** server, int argc, char** argv) {
  csiebox_server* tmp = (csiebox_server*)malloc(sizeof(csiebox_server));
  if (!tmp) {
    fprintf(stderr, "server malloc fail\n");
    return;
  }
  memset(tmp, 0, sizeof(csiebox_server));
  if (!parse_arg(tmp, argc, argv)) {
    fprintf(stderr, "Usage: %s [config file] [-d]\n", argv[0]);
    free(tmp);
    return;
  }

  int fd = server_start();
  if (fd < 0) {
    fprintf(stderr, "server fail\n");
    free(tmp);
    return;
  }
  tmp->client = (csiebox_client_info**)malloc(sizeof(csiebox_client_info*) * getdtablesize());
  if (!tmp->client) {
    fprintf(stderr, "client list malloc fail\n");
    close(fd);
    free(tmp);
    return;
  }
  memset(tmp->client, 0, sizeof(csiebox_client_info*) * getdtablesize());
  tmp->listen_fd = fd;
  *server = tmp;
}

pthread_mutex_t request_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  got_request   = PTHREAD_COND_INITIALIZER;
int task_exist, busy_num = 0;
fd_set busy;

typedef struct thread_data{
  csiebox_server* server;
  int conn_fd;
  int fdmax;
  fd_set* master;
} TRDdata;

char fifo_path[PATH_MAX];

void handle_term(int signum){
	fprintf(stderr, "signal termination\n");
	unlink(fifo_path);
	exit(0);
}

void handle_int(int signum){
	fprintf(stderr, "signal interruption\n");
	unlink(fifo_path);
	exit(0);
}

void handle_usr1(int signum){
	FILE* fp = fopen(fifo_path, "w");
	uint32_t thread_num = htonl(busy_num);
	fwrite(&thread_num, sizeof(uint32_t) , 1, fp);
	fclose(fp);
}

void daemonize(csiebox_server *server){
	int id;
	if((id = fork()) == 0){
		int pid = (int)getpid();
		char log_path[PATH_MAX];
		sprintf(log_path, "%s/log.txt", server->arg.run_path);
		int log_fd = open(log_path, O_CREAT | O_WRONLY | O_TRUNC);
		setsid();
		close(STDIN_FILENO);
		close(STDOUT_FILENO);
		dup2(log_fd, STDERR_FILENO);
		close(STDERR_FILENO);
		close(log_fd);
		char tmp[PATH_MAX];
		sprintf(tmp, "%s/csiebox_server.pid\0", server->arg.run_path);
		FILE *fp = fopen(tmp, "w");
		fprintf(fp, "%d", pid);
		fclose(fp);
	}else{
		exit(0);
	}
}

int csiebox_server_run(csiebox_server* server) { 
  if(server->arg.daemonize){
    fprintf(stderr, "Let's DAEMONIZE!!!\n");
  	daemonize(server);
	}
	
	int pid = getpid();
	sprintf(fifo_path, "%s/fifo.%d\0", server->arg.run_path, pid);
	mkfifo(fifo_path, 0666);

	struct sigaction sa_int, sa_term, sa_usr1;
	memset(&sa_int, 0, sizeof(struct sigaction));
	memset(&sa_term, 0, sizeof(struct sigaction));
	memset(&sa_usr1, 0, sizeof(struct sigaction));
	sa_int.sa_handler = handle_int;
	sa_term.sa_handler = handle_term;
	sa_usr1.sa_handler = handle_usr1;
	sigaction(SIGINT, &sa_int, NULL);
	sigaction(SIGTERM, &sa_term, NULL);
	sigaction(SIGUSR1, &sa_usr1, NULL);
	
  int conn_fd = 0, conn_len;
  struct sockaddr_in addr;
  int count = 0;

  fd_set master;
  fd_set read_fds;
  int i, fdmax;
  FD_ZERO(&master);
  FD_ZERO(&read_fds);
  FD_ZERO(&busy);
  FD_SET(server->listen_fd, &master);
  fdmax = server->listen_fd;
  
  pthread_t p_threads[server->arg.thread_num];

  TRDdata task_needed;
  task_needed.server = server;
  task_needed.conn_fd = conn_fd;
  task_needed.fdmax = fdmax;
  task_needed.master = &master;

  for (i = 0; i < server->arg.thread_num; i++) {
    pthread_create(&p_threads[i], NULL, thread_working, (void*)&task_needed);
    fprintf(stderr, "THREAD %d created.\n", i);
  }

  while (1) {
    read_fds = master;
    memset(&addr, 0, sizeof(addr));
    conn_len = 0;
fprintf(stderr, "\ndo select\n");
    if(select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1){
    	perror("select");
      	continue;
    }

    for(i = 0; i <= fdmax; i++){  
fprintf(stderr, "\ti in sub loop: %d\n", i);
      if(FD_ISSET(i, &read_fds)){
fprintf(stderr, "ISSET!!!\n");
        if(i == server->listen_fd){
          conn_len = sizeof(addr);
          // waiting client connect
          conn_fd = accept(server->listen_fd, (struct sockaddr*)&addr, (socklen_t*)&conn_len);
          if (conn_fd < 0) {
            if (errno == ENFILE) {
              fprintf(stderr, "out of file descriptor table\n");
              continue;
            } else if (errno == EAGAIN || errno == EINTR) {
              continue;
            } else {
              fprintf(stderr, "accept err\n");
              fprintf(stderr, "code: %s\n", strerror(errno));
              break;
            }
          } else {
            FD_SET(conn_fd, &master);
            if(conn_fd > fdmax)
              fdmax = conn_fd;
          }
        } else {
          if(FD_ISSET(i, &busy)){ // solve fast-select
            fprintf(stderr, "Already Doing!!!\n");
            sleep(1);
            continue;
          }else if(busy_num == server->arg.thread_num){
            fprintf(stderr, "BUSY!!!\n");
            handle_request(server, conn_fd, -100);
            continue;
          }
    // handle request from connected socket fd
fprintf(stderr, "do work distribution\n");
          task_exist = 1;
          task_needed.conn_fd = i;
          FD_SET(i, &busy);
          busy_num++;
          task_needed.fdmax = fdmax;
          pthread_cond_signal(&got_request);
          //sleep(3);
  //fprintf(stderr, "handle finished: %d\n", count++);
        }
      }
    }
  }
  return 1;
}


void csiebox_server_destroy(csiebox_server** server) {
  csiebox_server* tmp = *server;
  *server = 0;
  if (!tmp) {
    return;
  }
  close(tmp->listen_fd);
  free(tmp->client);
  free(tmp);
}

static int parse_arg(csiebox_server* server, int argc, char** argv) {
  if (argc < 2) {
    return 0;
  }
  FILE* file = fopen(argv[1], "r");
  if (!file) {
    return 0;
  }
  fprintf(stderr, "reading config...\n");
  if(argc == 3 && strcmp(argv[2], "-d") == 0){
    server->arg.daemonize = 1;
  }else
    server->arg.daemonize = 0;
  size_t keysize = 20, valsize = 20;
  char* key = (char*)malloc(sizeof(char) * keysize);
  char* val = (char*)malloc(sizeof(char) * valsize);
  ssize_t keylen, vallen;
  int accept_config_total = 3;
  int accept_config[4] = {0, 0, 0, 0};
  while ((keylen = getdelim(&key, &keysize, '=', file) - 1) > 0) {
    key[keylen] = '\0';
    vallen = getline(&val, &valsize, file) - 1;
    val[vallen] = '\0';
    fprintf(stderr, "config (%zd, %s)=(%zd, %s)\n", keylen, key, vallen, val);
    if (strcmp("path", key) == 0) {
      if (vallen <= sizeof(server->arg.path)) {
        strncpy(server->arg.path, val, vallen);
        accept_config[0] = 1;
      }
    } else if (strcmp("account_path", key) == 0) {
      if (vallen <= sizeof(server->arg.account_path)) {
        strncpy(server->arg.account_path, val, vallen);
        accept_config[1] = 1;
      }
    } else if (strcmp("thread", key) == 0) {
      server->arg.thread_num = strtol(val, NULL, 10);
      accept_config[2] = 1;
    } else if (strcmp("run_path", key) == 0) {
      if (vallen <= sizeof(server->arg.path)) {
        strncpy(server->arg.run_path, val, vallen);
        accept_config[3] = 1;
      }
    }
  }
  free(key);
  free(val);
  fclose(file);
  int i, test = 1;
  for (i = 0; i < accept_config_total; ++i) {
    test = test & accept_config[i];
  }
  if (!test) {
    fprintf(stderr, "config error\n");
    return 0;
  }
  return 1;
}

static int thread_working(void* data) {

  pthread_mutex_lock(&request_mutex);

  TRDdata *task = (TRDdata*)data;

  while (1) {
    if (task_exist) { /* a request is pending */
      task_exist = 0;
      if(handle_request(task->server, task->conn_fd, task->fdmax) == -1){
        logout(task->server, task->conn_fd);
        FD_CLR(task->conn_fd, task->master); //task->master == &master
        fprintf(stderr, "logout: %d\n", task->conn_fd);
      }
      //fprintf(stderr, "pretend busy...\n");
      //sleep(5);
      FD_CLR(task->conn_fd, &busy);
      busy_num--;
      fprintf(stderr, "--task fininsed-- conn_fd[%d]\n", task->conn_fd);
    }else {
      pthread_cond_wait(&got_request, &request_mutex);
    }
  }
}

static int handle_request(csiebox_server* server, int conn_fd, int fdmax) {
  int reject = 0;
  if(fdmax < 0)
    reject = 1;
  if(reject)
    fprintf(stderr, "Rejecting...\n");
  else
    fprintf(stderr, "Handling...\n");
  csiebox_protocol_header header;
  memset(&header, 0, sizeof(header));
  
  csiebox_protocol_header tmphd;
  memset(&tmphd, 0, sizeof(tmphd));
  tmphd.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES;
  tmphd.res.status = CSIEBOX_PROTOCOL_STATUS_BUSY;
  
  if(!recv_message(conn_fd, &header, sizeof(header))){
    return -1;
  }
  if (header.req.magic != CSIEBOX_PROTOCOL_MAGIC_REQ) {
    return 1;
  }
  switch (header.req.op) {
    
    case CSIEBOX_PROTOCOL_OP_LOGIN:
      if(reject) fprintf(stderr, "[REJECT]");
      fprintf(stderr, "login\n");
      csiebox_protocol_login req;
      if (complete_message_with_header(conn_fd, &header, &req)) {
        if(reject)
          send_message(conn_fd, &tmphd, sizeof(tmphd));
        else
          login(server, conn_fd, &req);
      }
      break;
    
    case CSIEBOX_PROTOCOL_OP_SYNC_META:
      if(reject) fprintf(stderr, "[REJECT]");
      fprintf(stderr, "sync meta\n");
      csiebox_protocol_meta meta;
      if (complete_message_with_header(conn_fd, &header, &meta)) {
        if(reject){
          char tmp[PATH_MAX];
          recv_message(conn_fd, tmp, meta.message.body.pathlen);
          send_message(conn_fd, &tmphd, sizeof(tmphd));
        }else
          sync_file(server, conn_fd, &meta, fdmax);
      }
      break;
    
    case CSIEBOX_PROTOCOL_OP_SYNC_END:
      fprintf(stderr, "sync end\n");
    break;
    
    case CSIEBOX_PROTOCOL_OP_RM:
      if(reject) fprintf(stderr, "[REJECT]");
      fprintf(stderr, "rm\n");
      csiebox_protocol_rm rm;
      if (complete_message_with_header(conn_fd, &header, &rm)) {
        if(reject){
          char tmp[PATH_MAX];
          recv_message(conn_fd, tmp, rm.message.body.pathlen);
          send_message(conn_fd, &tmphd, sizeof(tmphd));
        }else
          rm_file(server, conn_fd, &rm, fdmax);
      }
      break;
    default:
      fprintf(stderr, "unknow op %x\n", header.req.op);
      break;
    }  
  return 1;
}

static int get_account_info(
  csiebox_server* server,  const char* user, csiebox_account_info* info) {
  FILE* file = fopen(server->arg.account_path, "r");
  if (!file) {
    return 0;
  }
  size_t buflen = 100;
  char* buf = (char*)malloc(sizeof(char) * buflen);
  memset(buf, 0, buflen);
  ssize_t len;
  int ret = 0;
  int line = 0;
  while ((len = getline(&buf, &buflen, file) - 1) > 0) {
    ++line;
    buf[len] = '\0';
    char* u = strtok(buf, ",");
    if (!u) {
      fprintf(stderr, "ill form in account file, line %d\n", line);
      continue;
    }
    if (strcmp(user, u) == 0) {
      memcpy(info->user, user, strlen(user));
      char* passwd = strtok(NULL, ",");
      if (!passwd) {
        fprintf(stderr, "ill form in account file, line %d\n", line);
        continue;
      }
      md5(passwd, strlen(passwd), info->passwd_hash);
      ret = 1;
      break;
    }
  }
  free(buf);
  fclose(file);
  return ret;
}


static void login(csiebox_server* server, int conn_fd, csiebox_protocol_login* login) {
  int succ = 1;
  csiebox_client_info* info = (csiebox_client_info*)malloc(sizeof(csiebox_client_info));
  memset(info, 0, sizeof(csiebox_client_info));
  if (!get_account_info(server, login->message.body.user, &(info->account))) {
    fprintf(stderr, "cannot find account\n");
    succ = 0;
  }
  if (succ &&
      memcmp(login->message.body.passwd_hash,
             info->account.passwd_hash,
             MD5_DIGEST_LENGTH) != 0) {
    fprintf(stderr, "passwd miss match\n");
    succ = 0;
  }

  csiebox_protocol_header header;
  memset(&header, 0, sizeof(header));
  header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES;
  header.res.op = CSIEBOX_PROTOCOL_OP_LOGIN;
  header.res.datalen = 0;
  if (succ) {
    if (server->client[conn_fd]) {
      free(server->client[conn_fd]);
    }
    info->conn_fd = conn_fd;
    server->client[conn_fd] = info;
    header.res.status = CSIEBOX_PROTOCOL_STATUS_OK;
    header.res.client_id = info->conn_fd;
    char* homedir = get_user_homedir(server, info);
    mkdir(homedir, DIR_S_FLAG);
    free(homedir);
  } else {
    header.res.status = CSIEBOX_PROTOCOL_STATUS_FAIL;
    free(info);
  }
  send_message(conn_fd, &header, sizeof(header));
}

static void logout(csiebox_server* server, int conn_fd) {
  free(server->client[conn_fd]);
  server->client[conn_fd] = 0;
  close(conn_fd);
}

static void sync_file(csiebox_server* server, int conn_fd, csiebox_protocol_meta* meta, int fdmax) {
  csiebox_client_info* info = server->client[conn_fd];
  char* homedir = get_user_homedir(server, info);
  printf("homedir = %s\n", homedir);
  char buf[PATH_MAX], req_path[PATH_MAX];
  memset(buf, 0, PATH_MAX);
  memset(req_path, 0, PATH_MAX);
  recv_message(conn_fd, buf, meta->message.body.pathlen);
  
  char relpath[PATH_MAX];
  strcpy(relpath, buf);
  
  sprintf(req_path, "%s%s", homedir, buf);
  free(homedir);
  fprintf(stderr, "req_path: %s\n", req_path);
  struct stat stat;
  memset(&stat, 0, sizeof(struct stat));
  int need_data = 0, change = 0, chdmeta = 0, block = 0;
  if (lstat(req_path, &stat) < 0) {
    need_data = 1;
    change = 1;
  } else { 					
    if(stat.st_mode != meta->message.body.stat.st_mode) { 
      chmod(req_path, meta->message.body.stat.st_mode);
    }				
    if(stat.st_atime != meta->message.body.stat.st_atime ||
       stat.st_mtime != meta->message.body.stat.st_mtime){
      struct utimbuf* buf = (struct utimbuf*)malloc(sizeof(struct utimbuf));
      buf->actime = meta->message.body.stat.st_atime;
      buf->modtime = meta->message.body.stat.st_mtime;
      chdmeta = 1;
      if(utime(req_path, buf) != 0){
        printf("time fail\n");
      }
    }
    uint8_t hash[MD5_DIGEST_LENGTH];
    memset(hash, 0, MD5_DIGEST_LENGTH);
    if ((stat.st_mode & S_IFMT) == S_IFDIR) {
    } else {
      md5_file(req_path, hash);
    }
    if (memcmp(hash, meta->message.body.hash, MD5_DIGEST_LENGTH) != 0) {
      need_data = 1;
    }
  }
  struct flock lock = {F_WRLCK, SEEK_SET, 0, 0, 0};

  csiebox_protocol_header header;
  memset(&header, 0, sizeof(header));
  header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES;
  header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_META;
  header.res.datalen = 0;
  header.res.client_id = conn_fd;
  int fd; 
  if (need_data) {
    fd = open(req_path, O_CREAT | O_WRONLY | O_TRUNC, REG_S_FLAG);
    fcntl(fd, F_GETLK, &lock);
    if(lock.l_type != F_UNLCK){
      fprintf(stderr, "block\n");
      header.res.status = CSIEBOX_PROTOCOL_STATUS_BUSY;
      close(fd);
      block = 1;
    }else{
      header.res.status = CSIEBOX_PROTOCOL_STATUS_MORE;
      lock.l_type = F_WRLCK;
      fcntl(fd, F_SETLK, &lock);
    }
  } else {
    header.res.status = CSIEBOX_PROTOCOL_STATUS_OK;
  }
  send_message(conn_fd, &header, sizeof(header));
  if (!need_data){
    fprintf(stderr, "no need, finished.\n");
    return;
  }else if(block){
    return;
  }

  csiebox_protocol_file file;
  if (need_data) {
    memset(&file, 0, sizeof(file));
    recv_message(conn_fd, &file, sizeof(file));
    fprintf(stderr, "sync file: %zd\n", file.message.body.datalen);
    if ((meta->message.body.stat.st_mode & S_IFMT) == S_IFDIR) {
      fprintf(stderr, "dir\n");
      mkdir(req_path, DIR_S_FLAG);
    } else {
      fprintf(stderr, "regular file\n");
      //int fd = open(req_path, O_CREAT | O_WRONLY | O_TRUNC, REG_S_FLAG);
      size_t total = 0, readlen = 0;;
      char buf[4096];
      memset(buf, 0, 4096);
      while (file.message.body.datalen > total) {
        if (file.message.body.datalen - total < 4096) {
          readlen = file.message.body.datalen - total;
        } else {
          readlen = 4096;
        }
        if (!recv_message(conn_fd, buf, readlen)) {
          fprintf(stderr, "file broken\n");
          break;
        }
        total += readlen;
        if (fd > 0) {
          write(fd, buf, readlen);
        }
      }
      if (fd > 0) {
        close(fd);
      }
    }

    if (change) {
      chmod(req_path, meta->message.body.stat.st_mode);
      struct utimbuf* buf = (struct utimbuf*)malloc(sizeof(struct utimbuf));
      buf->actime = meta->message.body.stat.st_atime;
      buf->modtime = meta->message.body.stat.st_mtime;
      utime(req_path, buf);
    }

    header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_FILE;
    header.res.status = CSIEBOX_PROTOCOL_STATUS_OK;
    send_message(conn_fd, &header, sizeof(header));
    fprintf(stderr, "...fall asleep...\n");
    sleep(10);
    lock.l_type = F_UNLCK;
    fcntl(fd, F_SETLK, &lock);
  }
}

static char* get_user_homedir(csiebox_server* server, csiebox_client_info* info) {
  char* ret = (char*)malloc(sizeof(char) * PATH_MAX);
  memset(ret, 0, PATH_MAX);
  sprintf(ret, "%s/%s", server->arg.path, info->account.user);
  return ret;
}

static void rm_file(csiebox_server* server, int conn_fd, csiebox_protocol_rm* rm, int fdmax) {
  csiebox_client_info* info = server->client[conn_fd];
  char* homedir = get_user_homedir(server, info);
  char req_path[PATH_MAX], buf[PATH_MAX];
  memset(req_path, 0, PATH_MAX);
  memset(buf, 0, PATH_MAX);
  recv_message(conn_fd, buf, rm->message.body.pathlen);
  sprintf(req_path, "%s%s", homedir, buf);
  free(homedir);
  fprintf(stderr, "rm (%zd, %s)\n", strlen(req_path), req_path);
  
  int fd;
  csiebox_protocol_header header;
  memset(&header, 0, sizeof(header));
  struct flock lock = {F_WRLCK, SEEK_SET, 0, 0, 0};
  
  struct stat stat;
  memset(&stat, 0, sizeof(stat));
  lstat(req_path, &stat);
  if ((stat.st_mode & S_IFMT) != S_IFDIR){
    fd = open(req_path, O_RDONLY, REG_S_FLAG);
    fcntl(fd, F_GETLK, &lock);
    if(lock.l_type != F_UNLCK){
      fprintf(stderr, "block\n");
      header.res.status = CSIEBOX_PROTOCOL_STATUS_BUSY;
      close(fd);
      return;
    }else{
      lock.l_type = F_WRLCK;
      fcntl(fd, F_SETLK, &lock);
    }
  }

  // struct stat stat;
  // memset(&stat, 0, sizeof(stat));
  if(lstat(req_path, &stat) < 0){
    //csiebox_protocol_header header;
    memset(&header, 0, sizeof(header));
    header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES;
    header.res.op = CSIEBOX_PROTOCOL_OP_RM;
    header.res.datalen = 0;
    header.res.client_id = conn_fd;
    header.res.status = CSIEBOX_PROTOCOL_STATUS_OK;
    send_message(conn_fd, &header, sizeof(header));
    return;
  }
  fprintf(stderr, "RM: %s\n", req_path);
  if ((stat.st_mode & S_IFMT) == S_IFDIR) {
    rmdir(req_path);
  } else {
    unlink(req_path);
  }

  lock.l_type = F_UNLCK;
  fcntl(fd, F_SETLK, &lock);

  //csiebox_protocol_header header;
  //memset(&header, 0, sizeof(header));
  header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES;
  header.res.op = CSIEBOX_PROTOCOL_OP_RM;
  header.res.datalen = 0;
  header.res.client_id = conn_fd;
  header.res.status = CSIEBOX_PROTOCOL_STATUS_OK;
  send_message(conn_fd, &header, sizeof(header));

}
