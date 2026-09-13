#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/shm.h>
#include<stdbool.h>

//estrutura mensagem
typedef struct{
    long id_console;
    char string[100];
}Mensagem;
//estrutura chave
typedef struct{
  char key[33];
  int last_value,min,max,key_updates;
  double average;
  int alertado;
}Chave;

//estrutura sensor
typedef struct{
  char id[33];
}Sensor;

//estrutura alerta
typedef struct{
  char id[33];
  char chave[33];
  int min,max;
  int id_consola;
}Alerta;

//estrutura controlo
typedef struct{
  Sensor *sensor_ptr;
  Chave *chave_ptr;
  Alerta *alerta_ptr;
}Controlo;

//estrutura de apoio
typedef struct {
    int queue_sz, n_workers, max_keys, max_sensors, max_alerts;
} LeituraFile;


typedef struct{
  char linha[150];
  int console;
}Comando;

typedef struct{
  Comando atual;
  struct NodeInternalQueue *prox;
}NodeInternalQueue;

typedef struct{
  NodeInternalQueue * inicio;
  NodeInternalQueue * fim;
}InternalQueue;

//funcao de printar erros
void erro(char * erro){
  printf("ERRO: %s\n",erro);
}
