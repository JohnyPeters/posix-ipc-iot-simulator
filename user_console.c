//Autores:
//Alexandre Ferreira
//João Tinoco

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "structs.h"
#include <sys/stat.h>
#include <sys/msg.h>
#include <asm-generic/errno.h>
#include <errno.h>

#define CONSOLE_PIPE "/tmp/CONSOLE_PIPE"
#define KEY 123
int mqid;
int var=1;
pthread_t thread;
int id_thread=1;
int id_consola;

void ctrl_c_handler(){
    var=0;
    pthread_cancel(thread);
    printf("\nEND\n");
    exit(0);
}

void* receiver(){
    Mensagem msg;
    int check;
    while(var){
        check=(int) msgrcv(mqid, &msg, sizeof(Mensagem) - sizeof(long), id_consola, 0);
        if(check==-1){
            if(errno==EIDRM){
                printf("\nEND\n");
                exit(0);
            }else {
                printf("ERRO A LER DA MSG QUEUE\n");
            }
        }else {
            if (!strcmp(msg.string, "FIM")) {
                printf("\n");
            } else {
                printf("%s\n", msg.string);
            }
        }
    }
    pthread_exit(NULL);
}


int main(int argc,char *argv[]) {
    signal(SIGTSTP,SIG_IGN);
    //signal(SIGINT,ctrl_c_handler);
    //signal(SIGPIPE,ctrl_c_handler);
    mqid= msgget(KEY,IPC_CREAT|0777);
    if(mqid==-1){
        printf("Erro a criar msg queue\n");
        exit(0);
    }

    pthread_create(&thread,NULL,receiver,&id_thread);
  if(argc!=2){
    printf("Numero inválido de argumentos\nconsole <id>\n");
  }
  else if(atoi(argv[1])<1){
    printf("Identificador tem que ser superior a 0!\n");
  }
  else{
      id_consola=atoi(argv[1]);
    char identificador[30];
    strcpy(identificador, *(argv+1));
    printf("ID: %s\n",identificador);
    char comando[50];
    char *token;
    char linha[100];
    char delim[] = " ";
    char *id,*chave;
    int min,max;
    int flag;
    int check_write;

    int fd_console_pipe;
    if ((fd_console_pipe = open(CONSOLE_PIPE, O_WRONLY)) < 0) {
      erro("Cannot open console_pipe for writing");
      exit(0);
    }

    do {
        if (fgets(comando,50,stdin) == NULL) {
            erro("erro na leitura do comando");
            continue;
        }
        comando[strlen(comando)-1]='\0';
        if(strlen(comando)==0){
            continue;
        }
        token = strtok(comando, delim);

        //printf("%s",token);
        if (strcmp(token, "stats")==0) {
            //printf("KEY\tLast\tMin\tMax\tAvg\tCount\n");
            //stats();
            sprintf(linha,"c#stats#%d",atoi(argv[1]));
            check_write=write(fd_console_pipe, linha, sizeof(char)*strlen(linha));
            if(check_write<0){
                erro("erro na escrita");
            }
            /*do {
                msgrcv(mqid, &msg, sizeof(Mensagem) - sizeof(long), atoi(argv[1]), 0);
                if(!strcmp(msg.string,"FIM")){break;}
                printf("%s\n",msg.string);
            }while(1);
            printf("\n");*/
        } else if (strcmp(token, "reset")==0) {
            //printf("OK\n"); //fazer verificacao se a função executa corretamente
            sprintf(linha,"c#reset#%d",atoi(argv[1]));
            check_write=write(fd_console_pipe, linha, sizeof(char)*strlen(linha));
            if(check_write<0){
                erro("erro na escrita");
            }
            /*do {
                msgrcv(mqid, &msg, sizeof(Mensagem) - sizeof(long), atoi(argv[1]), 0);
                if(!strcmp(msg.string,"FIM")){break;}
                printf("%s\n",msg.string);
            }while(1);
            printf("\n");*/
            //reset();
        } else if (strcmp(token, "sensors")==0) {
            //sensors();
            sprintf(linha,"c#sensors#%d",atoi(argv[1]));
            check_write=write(fd_console_pipe, linha, sizeof(char)*strlen(linha));
            if(check_write<0){
                erro("erro na escrita");
            }
            /*do {
                msgrcv(mqid, &msg, sizeof(Mensagem) - sizeof(long), atoi(argv[1]), 0);
                if(!strcmp(msg.string,"FIM")){break;}
                printf("%s\n",msg.string);
            }while(1);
            printf("\n");*/
            //printf("ID\n");
        } else if (strcmp(token, "add_alert")==0) {
            int i=0;
            Alerta alerta;
            while (token!=NULL) {
                //printf("TOKENNN:%s\n",token);
                if (i == 1) {
                    id = token;
                    strcpy(alerta.id,id);
                    //printf("id:%s\n",id);
                }
                else if (i == 2) {
                    chave = token;
                    strcpy(alerta.chave,chave);
                    //printf("chave:%s\n",chave);
                }
                else if (i == 3) {
                    flag = 1;
                    for (int j = 0; (token[j] != '\0') ; j++) {
                        if (!isdigit(token[j])) {
                            flag = 0;
                            printf("O numero minimo nao e um numero.\n");
                            break;
                        }
                    }
                    if (flag == 1) {

                        min = atoi(token);
                        alerta.min=min;
                        //printf("%d\n",min);
                    }
                }
                else if (i == 4) {
                    flag = 1;
                    for (int j = 0; token[j] != '\0'; j++) {
                        if (!isdigit(token[j])) {
                            flag=0;
                            printf("O numero maximo nao e um numero.\n");
                            break;
                        }
                    }
                    if (flag == 1) {

                        max = atoi(token);
                        alerta.max=max;
                        //printf("%d\n", max);
                    }
                }
                i++;
                token = strtok(NULL, delim);

            }
            if (i != 5) {
                printf("Erro: argumentos invalidos para o comando 'add_alert'\n");
            } else {
                //printf("OK\n");
                sprintf(linha,"c#add_alert#%s#%s#%d#%d#%d",id,chave,atoi(argv[1]),min,max);
                check_write=write(fd_console_pipe, &linha, sizeof(linha));
                if(check_write<0){
                    erro("erro na escrita");
                }
                /*do {
                    msgrcv(mqid, &msg, sizeof(Mensagem) - sizeof(long), atoi(argv[1]), 0);
                    if(!strcmp(msg.string,"FIM")){break;}
                    printf("%s\n",msg.string);
                }while(1);
                printf("\n");*/
            }
        } else if (strcmp(token, "remove_alert")==0) {
            int i=0;
            while (token!=NULL){
                if(i==1)
                    id=token;
                //printf("%s",id);

                i++;
                token = strtok(NULL, delim);
            }
            if (i!= 2) {
                printf("Erro: argumentos invalidos para o comando 'remove_alert'\n");
            } else {
              //remove_alert(id);
              //printf("OK\n");
              sprintf(linha,"c#remove_alert#%s#%d",id,atoi(argv[1]));
                check_write=write(fd_console_pipe, &linha, sizeof(linha));
                if(check_write<0){
                    erro("erro na escrita");
                }
              /*do {
                  msgrcv(mqid, &msg, sizeof(Mensagem) - sizeof(long), atoi(argv[1]), 0);
                  if(!strcmp(msg.string,"FIM")){break;}
                  printf("%s\n",msg.string);
              }while(1);
              printf("\n");*/
            }
        } else if (strcmp(token, "list_alerts")==0) {
            //list_alerts();
            //printf("ID\tKEY MIN MAX\n");
            sprintf(linha,"c#list_alerts#%d",atoi(argv[1]));
            check_write=write(fd_console_pipe, &linha, sizeof(linha));
            if(check_write<0){
                erro("erro na escrita");
            }
            /*do {
                msgrcv(mqid, &msg, sizeof(Mensagem) - sizeof(long), atoi(argv[1]), 0);
                if(!strcmp(msg.string,"FIM")){break;}
                printf("%s\n",msg.string);
            }while(1);
            printf("\n");*/
        }
        else if (strcmp(token,"exit")==0) {
            break;  // sair do loop
        } else {
            printf("Comando invalido\n");
        }
    } while (1);
    ctrl_c_handler();
  }
  return 0;
}
