jsd: jsd.c
	gcc -Wall -pthread jsd.c strbuf.c -lm -o jsd
