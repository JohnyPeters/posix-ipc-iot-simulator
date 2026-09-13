//Autores:
//Alexandre Ferreira
//João Tinoco

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <stdbool.h>
#include "structs.h"
#include <errno.h>
#include <sys/stat.h>

#define CONSOLE_PIPE "/tmp/CONSOLE_PIPE"
#define SENSOR_PIPE "/tmp/SENSOR_PIPE"
#define KEY 123

void print_internalqueue(InternalQueue *fila);
void insert_internalqueue(InternalQueue *fila,char * linha,int sinal);
void destroy_internalqueue(InternalQueue *fila);
void clean_up();

//#define DEBUG //remove this line to remove debug messages


//inicialização de variaveis
int fecho=1;
int file_log;
int parent_pid;
int fd_console_pipe;
int queue_max;
int shmid;
char *name_file_log="log.txt";
Controlo * shared_var;
Controlo * mapa_shared_var;
int * array_estado_workers;
int *pipes;
int mqid;
//inicialização do mutex para escrita no ficheiro de log
pthread_mutex_t mutex_file_log =PTHREAD_MUTEX_INITIALIZER;
pthread_mutexattr_t att;


//semaforo da shared memory
sem_t * sem_shared_var;
//semaforo da internal queue
sem_t * sem_internal_queue;
//semaforo do dispatcher e dos workers
sem_t * sem_dispatcher;
//semaforo da thread console reader
sem_t * sem_console_reader;
//semaforo do dispatcher
sem_t * sem_add_dis;
//semaforo do watcher
sem_t * sem_watcher;

//threads
pthread_t reader_threads[3];
int * vetor_pids;
int n_workers;
int pid_watcher;
int n_keys;
int n_alertas;
int n_sensores;

int fd_sensor_pipe;
int fd_console_pipe;

InternalQueue *queue;

void create_internalqueue(InternalQueue *fila){
    sem_wait(sem_internal_queue);
    fila->inicio=NULL;
    fila->fim=NULL;
    sem_post(sem_internal_queue);
}

int check_internal_queue_size(InternalQueue * fila){
    NodeInternalQueue * temp;
    int contador=0;
    sem_wait(sem_internal_queue);
    temp=fila->inicio;
    while(temp!=NULL){
        contador++;
        temp= (NodeInternalQueue *) temp->prox;
    }
    sem_post(sem_internal_queue);
    if(contador==queue_max){
        return 1;
    }else{
        return 0;
    }
}

int empty_internalqueue(InternalQueue *fila){
    sem_wait(sem_internal_queue);
    if(fila->inicio==NULL){
        sem_post(sem_internal_queue);
        return 1;
    }else{
        sem_post(sem_internal_queue);
        return 0;
    }
}

void destroy_internalqueue(InternalQueue *fila){

    NodeInternalQueue * temp;
    while(!empty_internalqueue(fila)){
        sem_wait(sem_internal_queue);
        temp=fila->inicio;
        fila->inicio= (NodeInternalQueue *) fila->inicio->prox;
        free(temp);
        sem_post(sem_internal_queue);
    }
    sem_wait(sem_internal_queue);
    fila->fim=NULL;
    sem_post(sem_internal_queue);
}

void insert_internalqueue(InternalQueue *fila,char * linha,int sinal){
    NodeInternalQueue *new=(NodeInternalQueue *)malloc(sizeof(NodeInternalQueue));
    if(new!=NULL){
        strcpy(new->atual.linha,linha);
        new->atual.console=sinal;
        new->prox=NULL;
        if(empty_internalqueue(fila)){
            sem_wait(sem_internal_queue);
            fila->inicio=new;

        }else{
            sem_wait(sem_internal_queue);
            fila->fim->prox= (struct NodeInternalQueue *) new;
        }
        fila->fim=new;
        sem_post(sem_internal_queue);
        sem_post(sem_add_dis);
    }
}

void pop_internalqueue(InternalQueue *fila,char * string){
    NodeInternalQueue *temp,*anterior;
    int flag=0;
    if(!empty_internalqueue(fila)){
        if(check_internal_queue_size(queue)){
            flag=1;
        }

        temp=fila->inicio;
        while(temp!=NULL){
            if(temp->atual.console && temp==fila->inicio){
                sem_wait(sem_internal_queue);
                strcpy(string,fila->inicio->atual.linha);
                free(fila->inicio);
                fila->inicio= (NodeInternalQueue *) fila->inicio->prox;
                sem_post(sem_internal_queue);
                if(empty_internalqueue(fila)){
                    fila->fim=NULL;
                }
                if(flag){
                    sem_post(sem_console_reader);
                    flag=0;
                }
                return;
            }
            else if(temp->atual.console){
                sem_wait(sem_internal_queue);
                strcpy(string,temp->atual.linha);
                anterior->prox=temp->prox;
                free(temp);
                if(flag){
                    sem_post(sem_console_reader);
                    flag=0;
                }
                sem_post(sem_internal_queue);
                return;
            }
            anterior=temp;
            temp=(NodeInternalQueue *) temp->prox;
        }
        sem_wait(sem_internal_queue);
        strcpy(string,fila->inicio->atual.linha);
        free(fila->inicio);
        fila->inicio= (NodeInternalQueue *) fila->inicio->prox;
        sem_post(sem_internal_queue);
        if(empty_internalqueue(fila)){
            fila->fim=NULL;
        }
        if(flag){
            sem_post(sem_console_reader);
            flag=0;
        }
        return;
    }
}

void print_node(NodeInternalQueue *node){
    printf("Elemento: %s\n",node->atual.linha);
}

void print_internalqueue(InternalQueue *fila){
    NodeInternalQueue * temp;
    temp=fila->inicio;
    if(!empty_internalqueue(fila)){
        while(temp!=NULL){
            print_node(temp);
            temp= (NodeInternalQueue *) temp->prox;
        }
    }
}



//funcao de leitura do ficheiro de configurações
int read_file(LeituraFile *leiturafile,char * name_file) {
    //leiturafile=(leiturafile*)malloc(sizeof(LeituraFile));
    FILE * f;


    // Abrir o arquivo em modo leitura ("r")
    f = fopen(name_file, "r");

    // Verificar se o arquivo foi aberto corretamente
    if (f == NULL) {
        printf("Erro ao abrir o ficheiro!\n");
        return 0;
    }

    // Ler os inteiros do arquivo e atribuí-los à struct
    int elementos_lidos = fscanf(f, "%d\n%d\n%d\n%d\n%d", &leiturafile->queue_sz, &leiturafile->n_workers, &leiturafile->max_keys, &leiturafile->max_sensors, &leiturafile->max_alerts);
    if (elementos_lidos != 5) {
        printf("Erro de conversão!\nOs valores tem que ser inteiros\n");
        return 0;
    }
    bool flag=false;
    if(leiturafile->queue_sz<1){
      printf("O tamanho da queue tem que ser maior ou igual a 1\n");
      flag=true;
    }
    if(leiturafile->n_workers<1){
      printf("O numero de workers tem que ser maior ou igual a 1\n");
      flag=true;
    }
    if(leiturafile->max_keys<1){
      printf("O numero maximo de chaves tem que ser maior ou igual a 1\n");
      flag=true;
    }
    if(leiturafile->max_sensors<1){
      printf("O numero maximo de sensores tem que ser maior ou igual a 1\n");
      flag=true;
    }
    if(leiturafile->max_alerts<0){
      printf("O numero maximo de alertas tem que ser maior ou igual a 0\n");
      flag=true;
    }
    if(flag){
      printf("Valores incorretos\n");
      return 0;
    }
    //printf("queue size: %d\nN_workers: %d\nMax_keys: %d\nMax_sensors: %d\nMax_alerts: %d\n", leiturafile->queue_sz, leiturafile->n_workers, leiturafile->max_keys, leiturafile->max_sensors, leiturafile->max_alerts);

    // Fecho do ficheiro
    fclose(f);
    return 1;
}

//funcao que abre ficheiro de log
int open_file(char *file_name){
  int f;
  f=open(file_name, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
  if(f==-1){
    erro("Ao abrir o ficheiro de log");
    exit(0);
  }
  return f;
}

//funcao de escrita no ficheiro de log
void write_file(int f, char *text){
    time_t currentTime;
    struct tm *timeInfo;
    char timeString[9];
    char temp[200];

    time(&currentTime);
    timeInfo = localtime(&currentTime);
    strftime(timeString, sizeof(timeString), "%H:%M:%S", timeInfo);
    sprintf(temp,"%s %s\n",timeString,text);
    printf("%s",temp);
    pthread_mutex_lock(&mutex_file_log);
    write(f,temp,strlen(temp));
    pthread_mutex_unlock(&mutex_file_log);

    //fflush(f);
    //fflush(stdout);
}

void ctrlc_worker_handler(){
    fecho=0;
    //printf("CTRLC Worker hanfler\n");
}

int add_id(char *id){
    int contador=0;
    sem_wait(sem_shared_var);
    while(contador<n_sensores){
        if(!strcmp((mapa_shared_var->sensor_ptr+contador)->id,id)){
            //printf("ID SENSOR JA EXISTENTE\n");
            sem_post(sem_shared_var);
            return 0;
        }
        contador++;
    }
    contador=0;
    while(contador<n_sensores){
        if(strlen((mapa_shared_var->sensor_ptr+contador)->id)==0){
            strcpy((mapa_shared_var->sensor_ptr+contador)->id,id);
            //printf("ID SENSOR ADICIONADO\n");
            sem_post(sem_shared_var);
            return 1;
        }
        contador++;
    }
    write_file(file_log,"MAX ID's REACHED");
    //printf("ID SENSOR NAO ADICIONADO\n");
    sem_post(sem_shared_var);
    return -1;
}

int add_chave(char* string){
    //int check_send;
    //printf("ADD_CHAVE: %s\n",string);
    int contador=0;
    char *token;
    char id[33],chave[33];
    int value;
    token=strtok(string,"#");
    strcpy(id,token);
    token=strtok(NULL,"#");
    strcpy(chave,token);
    token=strtok(NULL,"#");
    value=atoi(token);
    //printf("%s,%s,%d",id,chave,value);
    add_id(id);
    sem_wait(sem_shared_var);
    while(contador<n_keys){
        //printf("while_contador1\n");
        //token[strcspn(token,"\r\n")]='\0';
        //printf("%s : %s",(mapa_shared_var->chave_ptr+contador)->key,token);
        if(strlen((mapa_shared_var->chave_ptr+contador)->key)!=0) {
            if (!strcmp((mapa_shared_var->chave_ptr + contador)->key, chave)) {
                //printf("if1\n");
                token = strtok(NULL, "#");
                (mapa_shared_var->chave_ptr + contador)->key_updates++;
                (mapa_shared_var->chave_ptr + contador)->average *= (mapa_shared_var->chave_ptr +contador)->key_updates;
                (mapa_shared_var->chave_ptr + contador)->average += value;
                (mapa_shared_var->chave_ptr + contador)->average /=(mapa_shared_var->chave_ptr + contador)->key_updates + 1;
                (mapa_shared_var->chave_ptr + contador)->last_value = value;
                (mapa_shared_var->chave_ptr + contador)->alertado=1;
                if (value < (mapa_shared_var->chave_ptr + contador)->min) {
                    (mapa_shared_var->chave_ptr + contador)->min = value;
                }
                if (value > (mapa_shared_var->chave_ptr + contador)->max) {
                    (mapa_shared_var->chave_ptr + contador)->max = value;
                }
                //printf("Chave %s atualizada\n", string);
                sem_post(sem_shared_var);
                sem_post(sem_watcher);
                return 0;
            }
        }
        contador++;
    }
    contador=0;

    while(contador<n_keys) {
        //printf("while_contador2\n");
        if(strlen((mapa_shared_var->chave_ptr+contador)->key)==0){
            //printf("if2\n");
            //printf("chave: %s\n",token);
            strcpy((mapa_shared_var->chave_ptr+contador)->key, chave);
            //token=strtok(NULL,"#");
            //printf("value: %s\n",token);
            (mapa_shared_var->chave_ptr+contador)->key_updates=0;
            (mapa_shared_var->chave_ptr+contador)->average=value;
            (mapa_shared_var->chave_ptr+contador)->last_value=value;
            (mapa_shared_var->chave_ptr+contador)->min=value;
            (mapa_shared_var->chave_ptr+contador)->max=value;
            (mapa_shared_var->chave_ptr+contador)->alertado=1;
            //printf("Chave %s criada\n",string);
            sem_post(sem_shared_var);
            sem_post(sem_watcher);
            return 1;
        }
        contador++;
    }
    write_file(file_log,"MAX KEYS REACHED");
    //printf("Sem espaco disponivel nas CHAVES\n");
    sem_post(sem_shared_var);
    return -1;
}
int check_chave_exist(char *chave){
    int contador=0;
    sem_wait(sem_shared_var);
    while(contador<n_keys){
        if(!strcmp((mapa_shared_var->chave_ptr+contador)->key,chave)){
            //printf("CHAVE EXISTE\n");
            sem_post(sem_shared_var);
            return 1;
        }
        contador++;
    }
    //printf("CHAVE NAO EXISTE\n");
    sem_post(sem_shared_var);
    return 0;
}
int add_alerta(char * string){
    int check_send;
    Mensagem msg;
    int contador=0,id_consola,min,max;
    char * token;
    char id[33],chave[33];
    token=strtok(string,"#");
    token=strtok(NULL,"#");
    token=strtok(NULL,"#");
    strcpy(id,token);
    token=strtok(NULL,"#");
    strcpy(chave,token);
    token=strtok(NULL,"#");
    id_consola= atoi(token);
    token=strtok(NULL,"#");
    min= atoi(token);
    token=strtok(NULL,"#");
    max= atoi(token);
    msg.id_console=id_consola;


    if(check_chave_exist(chave)) {
        sem_wait(sem_shared_var);
        while (contador < n_alertas) {
            if (!strcmp((mapa_shared_var->alerta_ptr + contador)->id, id)) {
                //printf("ALERTA EXISTENTE\n");
                strcpy(msg.string,"ERROR");
                check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
                if(check_send==-1){
                    write_file(file_log,"WORKER: ERROR SENDING MSG");
                }
                strcpy(msg.string,"FIM");
                check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
                if(check_send==-1){
                    write_file(file_log,"WORKER: ERROR SENDING MSG");
                }
                sem_post(sem_shared_var);
                return 0;
            }
            contador++;
        }
        contador = 0;

        while (contador < n_alertas) {
            //printf("%d\n",(mapa_shared_var->alerta_ptr + contador)->id_consola);
            if (strlen((mapa_shared_var->alerta_ptr + contador)->id) == 0) {
                strcpy((mapa_shared_var->alerta_ptr + contador)->id,id);
                strcpy((mapa_shared_var->alerta_ptr + contador)->chave,chave);
                (mapa_shared_var->alerta_ptr + contador)->id_consola=id_consola;
                (mapa_shared_var->alerta_ptr + contador)->min=min;
                (mapa_shared_var->alerta_ptr + contador)->max=max;
                //printf("ALERTA CRIADO\n");
                strcpy(msg.string,"OK");
                check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
                if(check_send==-1){
                    write_file(file_log,"WORKER: ERROR SENDING MSG");
                }
                strcpy(msg.string,"FIM");
                check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
                if(check_send==-1){
                    write_file(file_log,"WORKER: ERROR SENDING MSG");
                }
                sem_post(sem_shared_var);
                sem_post(sem_watcher);
                return 1;
            }
            contador++;
        }
        write_file(file_log,"MAX ALERTS REACHED");
        //printf("SEM ESPACO DISPONIVEL\n");
        strcpy(msg.string,"ERROR");
        check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
        if(check_send==-1){
            write_file(file_log,"WORKER: ERROR SENDING MSG");
        }
        strcpy(msg.string,"FIM");
        check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
        if(check_send==-1){
            write_file(file_log,"WORKER: ERROR SENDING MSG");
        }
        sem_post(sem_shared_var);
        return -2;
    }
    //printf("CHAVE NAO EXISTE PARA ESTE ALERTA\n");
    strcpy(msg.string,"ERROR");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    strcpy(msg.string,"FIM");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    return -1;

}

void stats(int id_consola){
    int check_send;
    int contador=0;
    Mensagem msg;
    msg.id_console=id_consola;
    char string[100];
    sem_wait(sem_shared_var);
    while(contador<n_keys){
        if(strlen((mapa_shared_var->chave_ptr+contador)->key)!=0){
            sprintf(string,"%s %d %d %d %lf %d",(mapa_shared_var->chave_ptr+contador)->key,(mapa_shared_var->chave_ptr+contador)->last_value,(mapa_shared_var->chave_ptr+contador)->min,(mapa_shared_var->chave_ptr+contador)->max,(mapa_shared_var->chave_ptr+contador)->average,(mapa_shared_var->chave_ptr+contador)->key_updates);
            //printf("%s\n",string);
            strcpy(msg.string,string);
            check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
            if(check_send==-1){
                write_file(file_log,"WORKER: ERROR SENDING MSG");
            }
        }
        contador++;
    }
    strcpy(msg.string,"FIM");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    sem_post(sem_shared_var);
}
int remove_alert(char * id,int id_consola){
    int check_send;
    int contador=0;
    Mensagem msg;
    msg.id_console=id_consola;
    sem_wait(sem_shared_var);
    while(contador<n_alertas){
        if(strlen((mapa_shared_var->alerta_ptr+contador)->chave)!=0) {
            if (!strcmp((mapa_shared_var->alerta_ptr + contador)->id, id)){
                memset(mapa_shared_var->alerta_ptr+contador,0,sizeof(Alerta));
                //printf("ALERTA REMOVIDO\n");
                strcpy(msg.string,"OK");
                check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
                if(check_send==-1){
                    write_file(file_log,"WORKER: ERROR SENDING MSG");
                }
                strcpy(msg.string,"FIM");
                check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
                if(check_send==-1){
                    write_file(file_log,"WORKER: ERROR SENDING MSG");
                }
                sem_post(sem_shared_var);
                return 1;
            }
        }
        contador++;
    }
    //printf("ALERTA NAO ENCONTRADO\n");
    strcpy(msg.string,"ERROR");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    strcpy(msg.string,"FIM");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    sem_post(sem_shared_var);
    return 0;
}
void reset(int id){
    int check_send;
    Mensagem msg;
    msg.id_console=id;
    sem_wait(sem_shared_var);
    memset(mapa_shared_var->sensor_ptr,0,sizeof(Sensor)*n_sensores+sizeof(Chave)*n_keys+sizeof(Alerta)*n_alertas);
    strcpy(msg.string,"OK");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    strcpy(msg.string,"FIM");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    sem_post(sem_shared_var);
}
void list_alerts(int id_consola){
    int check_send;
    Mensagem msg;
    msg.id_console=id_consola;
    int contador=0;
    char linha[100];
    //printf("\n\n");
    sem_wait(sem_shared_var);
    while(contador<n_alertas){
        if(strlen((mapa_shared_var->alerta_ptr+contador)->chave)!=0){
            sprintf(linha,"%s %s %d %d",(mapa_shared_var->alerta_ptr+contador)->id,(mapa_shared_var->alerta_ptr+contador)->chave,(mapa_shared_var->alerta_ptr+contador)->min,(mapa_shared_var->alerta_ptr+contador)->max);
            //printf("%s\n",linha);
            strcpy(msg.string,linha);
            check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
            if(check_send==-1){
                write_file(file_log,"WORKER: ERROR SENDING MSG");
            }

        }
        contador++;
    }
    strcpy(msg.string,"FIM");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    sem_post(sem_shared_var);
}

void list_sensores(int id_consola){
    int check_send;
    Mensagem msg;
    msg.id_console=id_consola;
    int contador=0;
    //printf("\n\n");
    sem_wait(sem_shared_var);
    while(contador<n_sensores){
        if(strlen((mapa_shared_var->sensor_ptr+contador)->id)!=0){
            //printf("%s\n",(mapa_shared_var->sensor_ptr+contador)->id);
            strcpy(msg.string,(mapa_shared_var->sensor_ptr+contador)->id);
            check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
            if(check_send==-1){
                write_file(file_log,"WORKER: ERROR SENDING MSG");
            }

        }
        contador++;
    }
    strcpy(msg.string,"FIM");
    check_send=msgsnd(mqid,&msg,sizeof(Mensagem)-sizeof(long),0);
    if(check_send==-1){
        write_file(file_log,"WORKER: ERROR SENDING MSG");
    }
    sem_post(sem_shared_var);
}
//funcao do processo worker
void *worker(int id){
    signal(SIGTSTP,SIG_IGN);
    struct sigaction sa;
    sa.sa_handler = ctrlc_worker_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
  //printf("Worker %d lancado\n",id);
  sigaction(SIGINT,&sa,NULL);
  char temp[30];
  int check_read;
  int id_consola;
  char id_alerta[33];
  char * token;
  char linha[100];
  char copia_linha[100];
  char aux[100];
  sprintf(temp,"WORKER %d READY",id);
  write_file(file_log,temp);
  while(fecho) {
      memset(linha,0,sizeof(linha));
      sprintf(aux,"WORKER %d AVAILABLE TO WORK", id);
      write_file(file_log,aux);
      //printf("WORKER %d ATIVO\n",id);
      check_read = (int) read(pipes[(id-1) * 2 ], linha, sizeof(linha));
      strcpy(copia_linha,linha);
      //printf("WORKER %d RECEBEU %s\n",id,linha);
      sem_wait(sem_dispatcher);
      if (check_read > 0) {
          sprintf(aux,"WORKER %d RECEIVED A TASK", id);
          write_file(file_log,aux);
          token=strtok(copia_linha,"#");
          if(!strcmp("c",token)) {
              token = strtok(NULL, "#");
              if (!strcmp("add_alert", token)) {
                  add_alerta(linha);
              } else if (!strcmp("remove_alert", token)) {
                  token = strtok(NULL, "#");
                  strcpy(id_alerta, token);
                  token = strtok(NULL, "#");
                  id_consola = atoi(token);
                  remove_alert(id_alerta, id_consola);
              } else if (!strcmp("stats", token)) {
                  token = strtok(NULL, "#");
                  stats(atoi(token));
              } else if (!strcmp("reset", token)) {
                  token = strtok(NULL, "#");
                  reset(atoi(token));
              } else if (!strcmp("list_alerts", token)) {
                  token = strtok(NULL, "#");
                  list_alerts(atoi(token));
              } else if (!strcmp("sensors", token)) {
                  token = strtok(NULL, "#");
                  list_sensores(atoi(token));
              }
          }else{
              //printf("WORKER %d PROCESSA: %s\n", id, linha);
              add_chave(linha);

          }
          //sleep(2);
          sem_wait(sem_shared_var);
          //printf("LIBERTOU O SEU ESTADO\n");
          array_estado_workers[id-1] = 0;
          sem_post(sem_shared_var);


      }else{
          if(fecho) {
              sprintf(aux, "WORKER %d: ERROR RECEIVING FROM PIPE", id);
              write_file(file_log, aux);
          }
      }
      sem_post(sem_dispatcher);

  }
  //printf("WORKER %d ACABEI\n",id);
  return 0;
}

//funcao do processo watcher
void *watcher(){
    int check_send;
    signal(SIGTSTP,SIG_IGN);
    Mensagem msg;
    char linha[150];
    struct sigaction sa;
    sa.sa_handler = ctrlc_worker_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    int i=0,j=0;
  //printf("Watcher lancado\n");
    write_file(file_log,"PROCESS ALERTS_WATCHER CREATED");
   sigaction(SIGINT,&sa,NULL);
  while(fecho){
    sem_wait(sem_watcher);
    sem_wait(sem_shared_var);
    while(i<n_keys){
        if(strlen((mapa_shared_var->chave_ptr+i)->key)!=0) {
            while (j < n_alertas) {
                if(strlen((mapa_shared_var->alerta_ptr+j)->id)!=0) {
                    if(!strcmp((mapa_shared_var->alerta_ptr+j)->chave,(mapa_shared_var->chave_ptr+i)->key)){
                        if((mapa_shared_var->chave_ptr+i)->alertado) {
                            if ((mapa_shared_var->chave_ptr + i)->last_value > (mapa_shared_var->alerta_ptr + j)->max ||
                                (mapa_shared_var->chave_ptr + i)->last_value < (mapa_shared_var->alerta_ptr + j)->min) {
                                msg.id_console = (mapa_shared_var->alerta_ptr + j)->id_consola;

                                sprintf(linha, "ALERT %s (%s %d TO %d) TRIGGERED",
                                        (mapa_shared_var->alerta_ptr + j)->id, (mapa_shared_var->alerta_ptr + j)->chave,
                                        (mapa_shared_var->alerta_ptr + j)->min, (mapa_shared_var->alerta_ptr + j)->max);
                                strcpy(msg.string, linha);
                                check_send = msgsnd(mqid, &msg, sizeof(Mensagem) - sizeof(long), 0);
                                if (check_send == -1) {
                                    write_file(file_log, "ALERTS_WATCHER: ERROR SENDING MSG");
                                }
                                strcpy(msg.string, "FIM");
                                check_send = msgsnd(mqid, &msg, sizeof(Mensagem) - sizeof(long), 0);
                                write_file(file_log, linha);
                            }
                            (mapa_shared_var->chave_ptr+i)->alertado=0;
                        }
                    }
                }
                j++;
            }
            j=0;
        }
        i++;
    }
    i=0;
    sem_post(sem_shared_var);

  }
    //printf("WATCHER ACABEI\n");
  return 0;
}

//funcao da thread sensor_reader
void *sensor(){
  //Comando * comando;
  //comando.sinal=1;
  char linha[100];
  int check_read;
  //write_file(file_log,"THREAD SENSOR_READER CREATED");
  if ((fd_sensor_pipe = open(SENSOR_PIPE, O_RDONLY)) < 0) {
    write_file(file_log,"SENSOR_READER: Cannot open sensor_pipe for reading");
    kill(0,SIGINT);
    clean_up();
    exit(0);
  }

  while(fecho){
      memset(linha,0,sizeof(linha));
      check_read = (int)read(fd_sensor_pipe, linha, sizeof(linha));
      if(!check_internal_queue_size(queue)){
          if (check_read > 0) {
              //printf("sensor: %s\n", linha);
              insert_internalqueue(queue,linha,0);
          }else{
              if ((fd_sensor_pipe = open(SENSOR_PIPE, O_RDONLY)) < 0) {
                  write_file(file_log,"SENSOR_READER: Cannot open sensor_pipe for reading");
                  kill(0,SIGINT);
                  clean_up();
                  exit(0);
              }
          }
      }else{
          write_file(file_log,"SENSOR READER: INTERNAL QUEUE IS FULL");
      }

  }
  //printf("Sensor lancado\n");
  //printf("SENSOR ACABEI\n");
  pthread_exit(NULL);
  return 0;
}

//funcao da thread console_reader
void *console(){
    int check_read;
    char linha[100];
    //write_file(file_log,"THREAD SENSOR_READER CREATED");
    if ((fd_console_pipe = open(CONSOLE_PIPE, O_RDONLY)) < 0) {
        write_file(file_log,"CONSOLE_READER: Cannot open console_pipe for reading");
        kill(0,SIGINT);
        clean_up();
        exit(0);
    }

    while(fecho){
        memset(linha,0,sizeof(linha));
        check_read = (int)read(fd_console_pipe, linha, sizeof(linha));
        sem_wait(sem_console_reader);

        if (check_read > 0) {
            //printf("consola: %s\n", linha);
            insert_internalqueue(queue,linha,1);
        }else{
            if ((fd_console_pipe = open(CONSOLE_PIPE, O_RDONLY)) < 0) {
                write_file(file_log,"CONSOLE_READER: Cannot open console_pipe for reading");
                kill(0,SIGINT);
                clean_up();
                exit(0);
            }
        }
        if(!check_internal_queue_size(queue)){
            sem_post(sem_console_reader);
        }
    }
    //printf("CONSOLE ACABEI\n");
  pthread_exit(NULL);
  return 0;
}

//funcao da thread dispatcher
void *dispatcher(){
    char linha[100];
    int check_write;
    //char aux[150];
  //print("Dispatcher lancado");
  //write_file(file_log,"THREAD DISPATCHER CREATED");
    while(fecho) {
        sem_wait(sem_add_dis);
        if(!empty_internalqueue(queue)) {
            sem_wait(sem_dispatcher);
            for (int i = 0; i < n_workers; i++) {
                sem_wait(sem_shared_var);
                if (array_estado_workers[i] == 0) {
                    //sprintf(aux,"DISPATCHER: ");
                    //write_file(file_log,"");
                    array_estado_workers[i] = 1;
                    pop_internalqueue(queue, linha);
                    //printf("DISPATCHER: %s\n",linha);
                    check_write=write(pipes[i * 2 + 1], linha, sizeof(linha));
                    if(check_write==-1){
                        write_file(file_log,"DISPATCHER: ERROR WRITING ON PIPE");
                    }
                    //envia tarefa ao worker disponivel
                    i = n_workers;
                }
                sem_post(sem_shared_var);
            }
            sem_post(sem_dispatcher);
        }
        pthread_testcancel();
    }
    //printf("DISPATCHER ACABEI\n");
  pthread_exit(NULL);
  return 0;
}

//
void sigint(int signum) {
  //Recolha dos processos workers e watcher
  //printf("Entrei no signal handler\n");
  if(getpid()==parent_pid) {
      fecho = 0;
      //printf("Entrei no signal handler\n");
      write_file(file_log,"SIGNAL SIGINT RECEIVED");
      write_file(file_log,"HOME_IOT SIMULATOR WAITING FOR LAST TASKS TO FINISH");
      write_file(file_log,"HOME_IOT SIMULATOR CLOSING");
  }
    /*printf("Entrei no signal handler\n");
    for(int i=0;i<n_workers;i++){
      wait(&vetor_pids[i]);
    }
    #ifdef DEBUG
    printf("Receiving Watcher\n");
    #endif
    wait(&pid_watcher);

    unlink(SENSOR_PIPE);
    unlink(CONSOLE_PIPE);
    #ifdef DEBUG
    printf("Receiving threads\n");
    #endif
    //recolha threads
    //pthread_cancel(reader_threads[0]);
    //pthread_cancel(reader_threads[1]);
    //pthread_cancel(reader_threads[2]);

    pthread_join(reader_threads[0], NULL);
    pthread_join(reader_threads[1], NULL);
    pthread_join(reader_threads[2],NULL);

    //print_internalqueue(queue);

  //fecho do ficheiro de log
  write_file(file_log,"HOME_IOT SIMULATOR CLOSING");
  close(file_log);
  #ifdef DEBUG
  printf("Resources destruction\n");
  #endif
  //Libertacao dos recursos
  sem_close(sem_shared_var);
  sem_unlink("SEM_SHARED_VAR");
  sem_close(sem_internal_queue);
  sem_unlink("SEM_INTERNAL_QUEUE");
  sem_close(sem_dispatcher);
  sem_unlink("SEM_DISPATCHER");
  sem_close(sem_console_reader);
  sem_unlink("SEM_CONSOLE_READER");
  destroy_internalqueue(queue);
  pthread_mutex_destroy(&mutex_file_log);
  shmdt(shared_var);
  shmctl(shmid,IPC_RMID,NULL);
  exit(0);
}*/
}
void clean_up(){


    //pthread_join(reader_threads[0], NULL);
    //pthread_join(reader_threads[1], NULL);
    //pthread_join(reader_threads[2],NULL);

    for(int i=0;i<n_workers;i++){
        wait(&vetor_pids[i]);
    }
#ifdef DEBUG
    printf("Receiving Watcher\n");
#endif
    wait(&pid_watcher);
    //printf("RECOLHEU OS PROCESSOS\n");

    unlink(SENSOR_PIPE);
    unlink(CONSOLE_PIPE);
#ifdef DEBUG
    printf("Receiving threads\n");
#endif
    //recolha threads
    //pthread_cancel(reader_threads[0]);
    //pthread_cancel(reader_threads[1]);
    //pthread_cancel(reader_threads[2]);

    pthread_cancel(reader_threads[0]);
    pthread_cancel(reader_threads[1]);
    pthread_cancel(reader_threads[2]);


    //print_internalqueue(queue);

    //fecho do ficheiro de log

    close(file_log);
#ifdef DEBUG
    printf("Resources destruction\n");
#endif
    //Libertacao dos recursos
    sem_close(sem_shared_var);
    sem_unlink("SEM_SHARED_VAR");
    sem_close(sem_dispatcher);
    sem_unlink("SEM_DISPATCHER");
    sem_close(sem_console_reader);
    sem_unlink("SEM_CONSOLE_READER");
    destroy_internalqueue(queue);
    sem_close(sem_internal_queue);
    sem_unlink("SEM_INTERNAL_QUEUE");
    sem_close(sem_add_dis);
    sem_unlink("SEM_ADD_DIS");
    sem_close(sem_watcher);
    sem_unlink("SEM_WATCHER");
    pthread_mutex_destroy(&mutex_file_log);
    shmdt(shared_var);
    shmctl(shmid,IPC_RMID,NULL);
    msgctl(mqid,IPC_RMID,NULL);
}

void handler_ctrlz(){
    write_file(file_log, "SIGNAL SIGTSTP RECEIVED");
}

//funcao main
int main(int argc,char *argv[]){
    signal(SIGTSTP,handler_ctrlz);
    struct sigaction sa;
    sa.sa_handler = sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

  parent_pid=getpid();
  //printf("Main pid: %d\n",parent_pid);



  //verificacao dos parametros passados
  if(argc!=2){
    erro("Argumento invalido");
    return 0;
  }


  LeituraFile leitura;
  //leitura ficheiro de configuracao
  if(read_file(&leitura,argv[1])){
      //printf("Ficheiro lido com sucesso\n");
  }else{
    erro("Erro ao ler ficheiro");
    return 0;
  }
  n_workers=leitura.n_workers;
  n_alertas=leitura.max_alerts;
  n_keys=leitura.max_keys;
  n_sensores=leitura.max_sensors;
  queue_max=leitura.queue_sz;

    //signal(SIGINT, SIG_IGN);
    mqid= msgget(KEY,IPC_CREAT|0777);
    if(mqid==-1){
        erro("A criar msg queue\n");
        exit(0);
    }


  //abertura do ficheiro de log
  file_log=open_file(name_file_log);

  write_file(file_log,"HOME_IOT SIMULATOR STARTING");

  int pipes_temp[n_workers][2];

  for (int i = 0; i < n_workers; i++) {
      if (pipe(pipes_temp[i]) < 0) {
          erro("pipe error");
          exit(1);
      }
  }
  pipes= (int *) pipes_temp;

  pthread_mutexattr_init(&att);
  pthread_mutexattr_setpshared(&att, PTHREAD_PROCESS_SHARED);
  pthread_mutex_init(&mutex_file_log, &att);

  //criacao do semaforo da shared memory
  sem_unlink("SEM_SHARED_VAR");
  sem_shared_var=sem_open("SEM_SHARED_VAR",O_CREAT|O_EXCL,0700,1);
  //criacao do semaforo da shared memory
  sem_unlink("SEM_INTERNAL_QUEUE");
  sem_internal_queue=sem_open("SEM_INTERNAL_QUEUE",O_CREAT|O_EXCL,0700,1);
  //criacao do semaforo da shared memory
  sem_unlink("SEM_DISPATCHER");
  sem_dispatcher=sem_open("SEM_DISPATCHER",O_CREAT|O_EXCL,0700,n_workers);
  //criacao do semaforo da shared memory
  sem_unlink("SEM_CONSOLE_READER");
  sem_console_reader=sem_open("SEM_CONSOLE_READER",O_CREAT|O_EXCL,0700,1);
  //criacao do semaforo da shared memory
  sem_unlink("SEM_ADD_DIS");
  sem_add_dis=sem_open("SEM_ADD_DIS",O_CREAT|O_EXCL,0700,0);
  //criacao do semaforo da shared memory
  sem_unlink("SEM_WATCHER");
  sem_watcher=sem_open("SEM_WATCHER",O_CREAT|O_EXCL,0700,0);

  // Creates the console_named pipe if it doesn't exist yet
  unlink(SENSOR_PIPE);
  unlink(CONSOLE_PIPE);

  if ((mkfifo(SENSOR_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!=EEXIST)) {
    erro("Cannot create sensor_pipe");
    exit(0);
  }

  if ((mkfifo(CONSOLE_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!=EEXIST)) {
    erro("Cannot create console_pipe");
    exit(0);
}



  queue=(InternalQueue *)malloc(sizeof(InternalQueue));
  create_internalqueue(queue);


  #ifdef DEBUG
  printf("Creating shared memory\n");
  #endif

  // Create shared memory
  //sem_wait(sem_shared_var);
  shmid=shmget(IPC_PRIVATE,sizeof(int)*leitura.n_workers+sizeof(Controlo)+sizeof(Sensor)*leitura.max_sensors+sizeof(Chave)*leitura.max_keys+sizeof(Alerta)*leitura.max_alerts,IPC_CREAT | 0777);
  if(shmid<0){
      erro("creating shared memory");
      exit(0);
  }
  if((array_estado_workers=(int*)shmat(shmid,NULL,0))==(int*)-1){
      erro("attaching shared memory");
      exit(0);
  }

  mapa_shared_var=(Controlo *)((void*)array_estado_workers+sizeof(int)*leitura.n_workers);
  mapa_shared_var->sensor_ptr=(Sensor  *) ((void*)array_estado_workers+sizeof(int)*leitura.n_workers+sizeof(Controlo));
  mapa_shared_var->chave_ptr=(Chave *) ((void*)array_estado_workers+sizeof(int)*leitura.n_workers+sizeof(Controlo)+sizeof(Sensor)*leitura.max_sensors);
  mapa_shared_var->alerta_ptr=(Alerta *) ((void*)array_estado_workers+sizeof(int)*leitura.n_workers+sizeof(Controlo)+sizeof(Sensor)*leitura.max_sensors+sizeof(Chave)*leitura.max_keys);
  //array_estado_workers=(int *) (shared_var+sizeof(Controlo)+sizeof(Sensor)*leitura.max_sensors+sizeof(Chave)*leitura.max_keys+sizeof(Alerta)*leitura.max_alerts);

  memset(mapa_shared_var->sensor_ptr,0,sizeof(Sensor)*leitura.max_sensors+sizeof(Chave)*leitura.max_keys+sizeof(Alerta)*leitura.max_alerts);
  //memset(mapa_shared_var->alerta_ptr,0,sizeof(Alerta)*leitura.max_alerts);
  //printf("tamanho alocado por shmget(): %lu\n", sizeof(Controlo) + sizeof(Sensor)*leitura.max_sensors + sizeof(Chave)*leitura.max_keys + sizeof(Alerta)*leitura.max_alerts);
  //printf("tamanho necessário para array_estado_workers: %lu\n", sizeof(int) * n_workers);
  memset(array_estado_workers,0,sizeof(int)*n_workers);
  //sem_post(sem_shared_var);

  //Pid's dos processos workers
  //int worker_pids[leitura.n_workers];
  vetor_pids=(int*) malloc(sizeof(int)*n_workers);

  #ifdef DEBUG
  printf("Creating threads\n");
  #endif
  //lancamento threads
  pthread_create(&reader_threads[0], NULL, sensor,NULL);
  write_file(file_log,"THREAD SENSOR_READER CREATED");
  pthread_create(&reader_threads[1], NULL, console,NULL);
  write_file(file_log,"THREAD CONSOLE_READER CREATED");
  pthread_create(&reader_threads[2], NULL, dispatcher,NULL);
  write_file(file_log,"THREAD DISPATCHER CREATED");

  #ifdef DEBUG
  printf("Creating worker processes\n");
  #endif
//lancamento dos processos workers
int flag=0;
  for(int i=0;i<leitura.n_workers;i++){
    if((flag=fork())==0){
      vetor_pids[i]=getpid();
      //printf("Worker pid: %d\n",vetor_pids[i]);
      worker(i+1);
      exit(1);
    }
    if(flag<0){
      erro("Erro na funcao fork");
      exit(0);
    }
  }
  #ifdef DEBUG
  printf("Creating watcher process\n");
  #endif
//processo watcher
  if((flag=fork())==0){
    pid_watcher=getpid();
    //printf("Watcher pid: %d\n",pid_watcher);
    watcher();

    exit(1);
  }
  if(flag<0){
    erro("Erro na funcao fork");
    exit(0);
  }

  //Redirects SIGINT to sigint()
  //signal(SIGINT, SIG_DFL);
  sigaction(SIGINT, &sa,NULL);
  //int sinal=1;
  //printf("Entrei no while(1)\n");
  //while(1){}
  //write_file(file_log,"HOME_IOT SIMULATOR WAITING FOR LAST TASKS TO FINISH");

   /* for(int i=0;i<n_workers;i++){
        wait(&vetor_pids[i]);
    }
#ifdef DEBUG
    printf("Receiving Watcher\n");
#endif
    wait(&pid_watcher);
    printf("RECOLHEU OS PROCESSOS\n");

    unlink(SENSOR_PIPE);
    unlink(CONSOLE_PIPE);
#ifdef DEBUG
    printf("Receiving threads\n");
#endif
    //recolha threads
    pthread_cancel(reader_threads[0]);
    pthread_cancel(reader_threads[1]);
    pthread_cancel(reader_threads[2]);

    //pthread_join(reader_threads[0], NULL);
    //pthread_join(reader_threads[1], NULL);
    //pthread_join(reader_threads[2],NULL);

    //print_internalqueue(queue);

    //fecho do ficheiro de log
    write_file(file_log,"HOME_IOT SIMULATOR CLOSING");
    close(file_log);
#ifdef DEBUG
    printf("Resources destruction\n");
#endif
    //Libertacao dos recursos
    sem_close(sem_shared_var);
    sem_unlink("SEM_SHARED_VAR");
    sem_close(sem_dispatcher);
    sem_unlink("SEM_DISPATCHER");
    sem_close(sem_console_reader);
    sem_unlink("SEM_CONSOLE_READER");
    destroy_internalqueue(queue);
    sem_close(sem_internal_queue);
    sem_unlink("SEM_INTERNAL_QUEUE");
    sem_close(sem_add_dis);
    sem_unlink("SEM_ADD_DIS");
    sem_close(sem_watcher);
    sem_unlink("SEM_WATCHER");
    pthread_mutex_destroy(&mutex_file_log);
    shmdt(shared_var);
    shmctl(shmid,IPC_RMID,NULL);
    msgctl(mqid,IPC_RMID,NULL);
   //kill(0,SIGINT);*/
   clean_up();
   return 0;
}
