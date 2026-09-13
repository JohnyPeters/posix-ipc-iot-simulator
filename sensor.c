//Autores:
//Alexandre Ferreira
//João Tinoco

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <ctype.h>
#include "structs.h"
#include <sys/stat.h>
#include <errno.h>

#define SENSOR_PIPE "/tmp/SENSOR_PIPE"
int contador=0;
int fd_sensor_pipe;
int check_write;
void ctrl_z_handler(){
    printf("\nVALORES GERADOS: %d\n",contador);
}
void ctrl_c_handler(){
    exit(0);
}

int main(int argc,char *argv[]){
    char buf[100];
  if(argc!=6){
    printf("Numero inválido de argumentos\nsensor <id> <interval> <key> <min value> <max value>\n");

  }else{
  	int i=0;
    int flag=1,flag_chave=1;


  if ((fd_sensor_pipe = open(SENSOR_PIPE, O_WRONLY)) < 0) {
    erro("Cannot open pipe for writing: ");
    exit(0);
  }

    while((*(argv+3))[i]!='\0'){
    	if(!( (((*(argv+3))[i])>=48 && ((*(argv+3))[i])<=57) || (((*(argv+3))[i])>=65 && ((*(argv+3))[i])<=90) || (((*(argv+3))[i])>=97 && ((*(argv+3))[i])<=122)|| ((*(argv+3))[i])==95)){
    	flag_chave=0;
    	break;
    	}
    	i++;
    }
    if((strlen((*(argv+1)))<3) || (strlen((*(argv+1)))>32)){
    	printf("O identificador de um sensor é um código alfanumérico com um tamanho mínimo de 3 caracteres e tamanho máximo de 32 caracteres\n");
    	flag=0;
    }

    if((strlen((*(argv+3)))<3) || (strlen((*(argv+3)))>32)){
    	printf("A chave é uma string com um tamanho mínimo de 3 caracteres e tamanho máximo de 32 caracteres\n");
    	flag=0;
    }

    if(flag_chave==0){
    	printf("A chave é uma string formada por uma combinação de dígitos, caracteres alfabéticos e underscore\n");
    	flag=0;
   }
    if(atoi(*(argv+2))<0){
      printf("O intervalo tem que ser maior que zero\n");
      flag=0;
    }


    if(atoi(*(argv+4))<0){
      printf("O valor inteiro minimo tem que ser maior que zero\n");
      flag=0;
    }
    if(atoi(*(argv+5))<0){
      printf("O valor inteiro maximo tem que ser maior que zero\n");
      flag=0;
    }

    if(atoi(*(argv+4))>atoi(*(argv+5))){
    	 printf("O valor inteiro mínimo tem que ser menor que o valor inteiro máximo\n");
    	 flag=0;
    }


    for (int i = 0; ((*(argv+2))[i] != '\0') ; i++) {
        if (!isdigit((*(argv+2))[i])) {
            flag=0;
            printf("O intervalo nao e um numero.\n");
            break;
        }
    }

    for (int i = 0; ((*(argv+4))[i] != '\0') ; i++) {
        if (!isdigit((*(argv+4))[i])) {
            flag=0;
            printf("O numero minimo nao e um numero.\n");
            break;
        }
    }


    for (int i = 0; ((*(argv+5))[i] != '\0') ; i++) {
        if (!isdigit((*(argv+5))[i])) {
            flag=0;
            printf("O numero maximo nao e um numero.\n");
            break;
        }
    }

    if(flag==1){

        int aleatorio;
        signal(SIGINT,ctrl_c_handler);
        signal(SIGTSTP,ctrl_z_handler);
        signal(SIGPIPE,ctrl_c_handler);
        //printf("Identificador do sensor: %s\n",*(argv+1));
        //printf("Intervalo entre envios: %s\n",*(argv+2));
        //printf("Chave: %s\n",*(argv+3));
        //printf("Valor inteiro mínimo a ser enviado: %s\n",*(argv+4));
        //printf("Valor inteiro máximo a ser enviado: %s\n",*(argv+5));
        while(1) {
            srand(time(NULL));
            aleatorio=atoi(*(argv+4))+rand() % (atoi(*(argv+5))-atoi(*(argv+4))+1);
            sprintf(buf,"%s#%s#%d",*(argv+1),*(argv+3),aleatorio);
            printf("%s\n",buf);
            check_write=write(fd_sensor_pipe, buf, sizeof(char) * strlen(buf));
            if(check_write<0){
                erro("erro na escrita");
            }
            contador++;
            sleep(atoi(*(argv+2)));
        }

    }
  }
}

//{identificador do sensor} {intervalo entre envios em segundos(>=0)} {chave} {valor inteiro mínimo a ser enviado} {valor inteiro máximo a ser enviado}
