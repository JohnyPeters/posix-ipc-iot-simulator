CC = gcc
CFLAGS = -Wall -g
THREAD= -pthread

all: home_iot console sensor

home_iot: system_manager.c
	$(CC) $(CFLAGS) $(THREAD) -o home_iot system_manager.c

console: user_console.c
	$(CC) $(CFLAGS) $(THREAD) -o console user_console.c

sensor: sensor.c
	$(CC) $(CFLAGS) $(THREAD) -o sensor sensor.c

clean:
	rm -f home_iot
	rm -f console
	rm -f sensor
