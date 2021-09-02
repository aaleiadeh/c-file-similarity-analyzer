#include<stdlib.h>
#include<stdio.h>
#include<unistd.h>
#include<string.h>
#include<math.h>
#include<ctype.h>
#include<fcntl.h>
#include<dirent.h>
#include<sys/stat.h>
#include<pthread.h>
#include"strbuf.h"

typedef struct node{
	strbuf_t word;
	int wordcount;
	double frequency;
	struct node *next;
}node;

typedef struct wfd{
	strbuf_t path;
	node* head;
	struct wfd *next;
	int size;
	int totalcount;
}wfd;

typedef struct{
	wfd* filehead;
	wfd* filetail;
	int size;
	pthread_mutex_t lock;
}repository;

typedef struct{
	wfd* f1;
	wfd* f2;
	double jsd;
	int combinedwordcount;
	pthread_mutex_t lock;
	int tail;
}comparisons;

typedef struct{
        strbuf_t* data;
        unsigned count;
        unsigned head;
	int size;
        int open;
	int bounded;
	int activethreads;//only for dthreads
        pthread_mutex_t lock;
        pthread_cond_t read_ready;
        pthread_cond_t write_ready;
}queue_t;

repository repo;
int numfthreads;
int numdthreads;
int numathreads;
int mainfin;
char* suffix;

int repoinit(repository* repo){
	repo->filehead = malloc(sizeof(wfd));
	repo->filetail = repo->filehead;
	sb_init(&repo->filehead->path, 16);
	repo->filehead->next = NULL;
	repo->filehead->head = malloc(sizeof(node));
	sb_init(&repo->filehead->head->word, 16);
	repo->filehead->head->next = NULL;
	pthread_mutex_init(&repo->lock, NULL);
	return 0;
}

wfd* addwfd(){
	wfd* list = malloc(sizeof(wfd));
	sb_init(&list->path, 16);
	list->next = NULL;
	list->head = malloc(sizeof(node));
	sb_init(&list->head->word, 16);
	list->head->next = NULL;
	return list;
}

int init(queue_t* q, int size, int bounded)//Any positive number = bounded. Otherwise, unbounded
{
	if(size <= 0) return 1;
	q->size = size;
	q->open = 1;
	q->data = malloc(sizeof(strbuf_t) * size);
	for(int i = 0; i < size; i++) sb_init(&q->data[i], 100);
        q->count = 0;
        q->head = 0;
	q->bounded = bounded;
        pthread_mutex_init(&q->lock, NULL);
	pthread_cond_init(&q->read_ready, NULL);
	pthread_cond_init(&q->write_ready, NULL);

        return 0;
}

int isfileordir(char* path){//1 = file. 2 = dir
        struct stat data;
        int err = stat(path, &data);
        if(err){
                perror(path);
                return 0;
        }

        if(S_ISREG(data.st_mode)) return 1;
        if(S_ISDIR(data.st_mode)) return 2;
        return 0;
}

int readfile(char* path, wfd* list){
	int fd = open(path, O_RDONLY);
	if(fd == -1){
		perror(path);
		return 1;
	}
	int bytesread;
	char buf[256];
	strbuf_t wordbuf;
	sb_init(&wordbuf, 256);
	int firstrun = 1;
	while((bytesread = read(fd, buf, 256)) > 0){
                for(int i = 0; i < bytesread; i++){
                        if(isspace(buf[i])){
                                if(wordbuf.used > 0)//Contains a full word ready to write
                                {
                                        node* ptr = list->head;
					while(ptr->next != NULL){
						if(strcmp(wordbuf.data, ptr->word.data) == 0){
							++ptr->wordcount;
							++list->totalcount;
							break;
						}
						ptr = ptr->next;
					}
					if(ptr->next == NULL){
						if(strcmp(wordbuf.data, ptr->word.data) == 0){
							++ptr->wordcount;
							++list->totalcount;
						}
						else{
							if(firstrun){
								sb_concat(&ptr->word, wordbuf.data);
								++ptr->wordcount;
								++list->size;
								++list->totalcount;
								firstrun = 0;
							}
							else{
								ptr->next = malloc(sizeof(node));
								sb_init(&ptr->next->word, 16);
								sb_concat(&ptr->next->word, wordbuf.data);
								++ptr->next->wordcount;
								++list->size;
								++list->totalcount;
								ptr->next->next = NULL;
							}
						}
					}
                                        sb_reset(&wordbuf);
                                }
                        }
                        else
                        {
                                sb_append(&wordbuf, buf[i]);
                        }
                }
        }
	node* ptr = list->head;
	for(int i = 0; i < list->size; i++){
		ptr->frequency = (double)(ptr->wordcount)/(double)(list->totalcount);
		ptr = ptr->next;
	}
	close(fd);
	list->next = addwfd();
	++repo.size;
	repo.filetail = list->next;
	sb_destroy(&wordbuf);
	return 0;
}

int qclose(queue_t* q){
	pthread_mutex_lock(&q->lock);
	q->open = 0;
	pthread_cond_broadcast(&q->read_ready);
	pthread_cond_broadcast(&q->write_ready);
	pthread_mutex_unlock(&q->lock);
	return 0;
}

int enqueue(queue_t* q, char* file){//FIFO
	pthread_mutex_lock(&q->lock);
	while(q->count == q->size && q->bounded == 1){
		pthread_cond_wait(&q->write_ready, &q->lock);
	}
	if(!q->open && q->bounded == 1){
		pthread_mutex_unlock(&q->lock);
		return 1;
	}
	if(q->count == q->size && q->bounded == 0){
		int size = q->size * 2;
		strbuf_t *ptr = realloc(q->data, sizeof(strbuf_t) * size);
		q->data = ptr;
		q->size = size;
		for(int i = q->count; i < size; i++) sb_init(&q->data[i], 100);
	}
	int index = q->head + q->count;
	if(index >= q->size) index -= q->size;
	sb_reset(&q->data[index]);
	sb_concat(&q->data[index], file);
	++q->count;
	pthread_mutex_unlock(&q->lock);
	pthread_cond_signal(&q->read_ready);
	return 0;
}

int dequeue(queue_t* q, strbuf_t* item, queue_t* fq){
	pthread_mutex_lock(&q->lock);
	if(q->bounded == 1){
		while(q->count == 0 && q->open){
			pthread_cond_wait(&q->read_ready, &q->lock);
		}
		if(!q->open && q->count == 0){
			pthread_mutex_unlock(&q->lock);
			return 1;
		}
	}
	else{
		while(q->count == 0 && q->activethreads){
			--q->activethreads;
			if(!q->activethreads){
				qclose(fq);
				pthread_mutex_unlock(&q->lock);
				pthread_cond_broadcast(&q->read_ready);
				return 1;
			}
			pthread_cond_wait(&q->read_ready, &q->lock);
			if(!q->activethreads){
				pthread_mutex_unlock(&q->lock);
				return 1;
			}
			++q->activethreads;
		}
	}
	sb_reset(item);
	sb_concat(item, q->data[q->head].data);
	--q->count;
	++q->head;
	if(q->head == q->size) q->head = 0;
	pthread_mutex_unlock(&q->lock);
	pthread_cond_signal(&q->write_ready);
	return 0;
}

void* fthread(void* queue){
	queue_t* q = (queue_t*) queue;
	strbuf_t fileptr;
	sb_init(&fileptr, 100);
	int status = 0;
	while(q->count || !mainfin || q->open){
		status = dequeue(q, &fileptr, NULL);
		if(status == 1){
			break;
		}
		printf("[F:%ld] PROCESSING: %s\n", pthread_self(), fileptr.data);
		pthread_mutex_lock(&repo.lock);
		wfd* ptr = repo.filetail;
		sb_concat(&ptr->path, fileptr.data);
		readfile(fileptr.data, ptr);
		pthread_mutex_unlock(&repo.lock);
	}
	sb_destroy(&fileptr);
	return NULL;
}

void* dthread(void* fdqueue){
	queue_t* queues =  (queue_t *) fdqueue;
	queue_t* fq = &queues[0];
	queue_t* q = &queues[1];
	strbuf_t dirptr;
	sb_init(&dirptr, 100);
	strbuf_t fileptr;
	sb_init(&fileptr, 100);
	while(q->activethreads){
		dequeue(q, &dirptr, fq);
		if(!q->activethreads) break;
		printf("[D:%ld] PROCESSING: %s\n", pthread_self(), dirptr.data);
		DIR* dir = opendir(dirptr.data);
		if(!dir){
			perror(dirptr.data);
		}
		struct dirent* de;
		while((de = readdir(dir))){
			if(de->d_name[0] == '.') continue;
			int length = strlen(de->d_name);
			int sfxlen = strlen(suffix);
			if(strcmp(&de->d_name[length-sfxlen], suffix) != 0) continue;
			sb_reset(&fileptr);
			sb_concat(&fileptr, dirptr.data);
			sb_append(&fileptr, '/');
			sb_concat(&fileptr, de->d_name);
			int fod = isfileordir(fileptr.data);
			if(fod == 1){
				enqueue(fq, fileptr.data);
			}
			if(fod == 2){
				enqueue(q, fileptr.data);
			}
		}
		closedir(dir);
	}
	sb_destroy(&dirptr);
	sb_destroy(&fileptr);
	return NULL;
}

void* athread(void* arg){
	comparisons* cmparr = (comparisons*) arg;
	while(cmparr->tail > -1){
		pthread_mutex_lock(&cmparr->lock);
		if(cmparr->tail == -1) break;
		comparisons* cmp = &cmparr[cmparr->tail];
		--cmparr->tail;
		pthread_mutex_unlock(&cmparr->lock);
		cmp->combinedwordcount = cmp->f1->totalcount + cmp->f2->totalcount;
		int found;
		double avgfreq;
		double kld1 = 0;
		double kld2 = 0;
		double jsd = 0;
		node* ptr1;
		node* ptr2;

		ptr1 = cmp->f1->head;
		for(int i = 0; i < cmp->f1->size; i++){
			found = 0;
			ptr2 = cmp->f2->head;
			for(int j = 0; j < cmp->f2->size; j++){
				if(strcmp(ptr1->word.data, ptr2->word.data) == 0){
					found = 1;
					avgfreq = (ptr1->frequency + ptr2->frequency) * .5;
					break;
				}
				ptr2 = ptr2->next;
			}
			if(!found){
				avgfreq = (ptr1->frequency) * .5;
			}
			kld1 += ptr1->frequency * (log2((ptr1->frequency/avgfreq)));
			ptr1 = ptr1->next;
		}
		ptr1 = cmp->f2->head;
		for(int i = 0; i < cmp->f2->size; i++){
			found = 0;
			ptr2 = cmp->f1->head;
			for(int j = 0; j < cmp->f1->size; j++){
				if(strcmp(ptr1->word.data, ptr2->word.data) == 0){
					found = 1;
					avgfreq = (ptr1->frequency + ptr2->frequency) * .5;
					break;
				}
				ptr2 = ptr2->next;
			}
			if(!found){
				avgfreq = (ptr1->frequency) * .5;
			}
			kld2 += ptr1->frequency * (log2((ptr1->frequency/avgfreq)));
			ptr1 = ptr1->next;
		}
		jsd = sqrt(((kld1+kld2)/2));
		cmp->jsd = jsd;
		//printf("[A:%ld] KLD1: %f KLD2: %f %s:%s JSD: %f\n", pthread_self(), kld1, kld2, cmp->f1->path.data, cmp->f2->path.data, jsd);
		printf("[A:%ld] PROCESSING: %s, %s\n", pthread_self(), cmp->f1->path.data, cmp->f2->path.data);
	}
	return NULL;
}

int setintopt(int* arg, char* par){
	char type = par[1];
	char* ptr = &par[2];
	for(int i = 0; i < strlen(ptr); i++){
		if(!isdigit(ptr[i])){
			printf("Parameter for -%c must be an integer\n", type);
			return 1;
		}
	}
	*arg = atoi(ptr);
	return 0;
}

int compare(const void* i, const void* j){//want descending so -1 if i > j
	comparisons* ptr1 = (comparisons*) i;
	comparisons* ptr2 = (comparisons*) j;
	if(ptr1->combinedwordcount > ptr2->combinedwordcount) return -1;
	if(ptr1->combinedwordcount < ptr2->combinedwordcount) return 1;
	return 0;
}

int main(int argc, char *argv[]){
	if(argc == 1){
		printf("Too few arguments\n");
		return EXIT_FAILURE;
	}
	strbuf_t* files = malloc(sizeof(strbuf_t) * argc);//Oversized for simplicity
	int filesarrsize = 0;
	repoinit(&repo);
	queue_t* queues = malloc(sizeof(queue_t) * 2);
	init(&queues[0], 10, 1);
	init(&queues[1], 10, 0);
	numfthreads = 1;
	numdthreads = 1;
	numathreads = 1;
	suffix = "";
	for(int i = 1; i < argc; i++){
		int length = strlen(argv[i]);
		if(argv[i][0] == '-'){//Potential Optional Arg
			if(length == 1) continue;
			if(length == 2){
				printf("Optional argument missing parameter\n");
				return EXIT_FAILURE;
			}
			if(argv[i][1] == 'f'){
				int err = setintopt(&numfthreads, argv[i]);
				if(err) return EXIT_FAILURE;
			}
			else if(argv[i][1] == 'd'){
				int err = setintopt(&numdthreads, argv[i]);
				if(err) return EXIT_FAILURE;
			}
			else if(argv[i][1] == 'a'){
				int err = setintopt(&numathreads, argv[i]);
				if(err) return EXIT_FAILURE;
			}
			else if(argv[i][1] == 's') suffix = &argv[i][2];
			else{
				printf("Invalid Optional Argument\n");
				return EXIT_FAILURE;
			}
		}
		else{//Potential file or dir
			sb_init(&files[filesarrsize], length);
			sb_concat(&files[filesarrsize], argv[i]);
			filesarrsize++;
		}
	}
	pthread_t ftid[numfthreads];
	pthread_t dtid[numdthreads];
	pthread_t atid[numathreads];
	queues[1].activethreads = numdthreads;
	for(int i = 0; i < numfthreads; i++){
		pthread_create(&ftid[i], NULL, fthread, &queues[0]);
	}

	int status;
	for(int i = 0; i < filesarrsize; i++){
		int fod = isfileordir(files[i].data);
		if(!fod) status = 1;
		if(fod == 1){//file
			enqueue(&queues[0], files[i].data);
		}
		if(fod == 2){//dir
			enqueue(&queues[1], files[i].data);
		}
	}
	mainfin = 1;

	for(int i = 0; i < numdthreads; i++){
		pthread_create(&dtid[i], NULL, dthread, queues);
	}
	for(int i = 0; i < numfthreads; i++){
		pthread_join(ftid[i], NULL);
	}
	for(int i = 0; i < numdthreads; i++){
		pthread_join(dtid[i], NULL);
	}
	/*wfd* wfdptr = repo.filehead;
	for(int i = 0; i < repo.size; i++){
		node* ptr = wfdptr->head;
		printf("ContentCheck: %s:\n", wfdptr->path.data);
		for(int j = 0; j < wfdptr->size; j++){
			printf("Word: %s Count: %d Total: %d Frequency: %f\n", ptr->word.data, ptr->wordcount, wfdptr->totalcount, ptr->frequency);
			ptr = ptr->next;
		}
		wfdptr = wfdptr->next;
	}*/

	if(repo.size < 2){
		fprintf(stderr, "Not enough files collected for comparison\n");
		return EXIT_FAILURE;
	}
	int size = (repo.size * .5)*(repo.size - 1);
	comparisons* tocompare = malloc(sizeof(comparisons) * size);
	tocompare->tail = size-1;
	pthread_mutex_init(&tocompare->lock, NULL);
	wfd* wfdptr = repo.filehead;
	int count = 0;
	for(int i = 0; i < repo.size; i++){
		wfd* temp = wfdptr->next;
		for(int j = i; j < repo.size-1; j++){
			tocompare[count].f1 = wfdptr;
			tocompare[count].f2 = temp;
			count++;
			temp = temp->next;
		}
		wfdptr = wfdptr->next;
	}

	for(int i = 0; i < numathreads; i++){
		pthread_create(&atid[i], NULL, athread, tocompare);
	}
	for(int i = 0; i < numathreads; i++){
		pthread_join(atid[i], NULL);
	}

	/*printf("PREQSORT:\n");
	for(int i = 0; i < size; i++){
		printf("JSD: %f FILES: %s, %s TOTALWORDCOUNT: %d\n", tocompare[i].jsd, tocompare[i].f1->path.data, tocompare[i].f2->path.data, tocompare[i].combinedwordcount);
	}*/
	qsort(tocompare, size, sizeof(comparisons), compare);
	//printf("POSTQSORT:\n");
	for(int i = 0; i < size; i++){
		printf("JSD: %f FILES: %s, %s TOTALWORDCOUNT: %d\n", tocompare[i].jsd, tocompare[i].f1->path.data, tocompare[i].f2->path.data, tocompare[i].combinedwordcount);
	}

	//core structures free() time
	for(int i = 0; i < queues[0].size; i++){
		sb_destroy(&queues[0].data[i]);
	}
	for(int i = 0; i < queues[1].size; i++){
		sb_destroy(&queues[1].data[i]);
	}
	free(queues[0].data);
	free(queues[1].data);
	pthread_mutex_destroy(&queues[0].lock);
	pthread_mutex_destroy(&queues[1].lock);
	pthread_cond_destroy(&queues[0].read_ready);
	pthread_cond_destroy(&queues[0].write_ready);
	pthread_cond_destroy(&queues[1].read_ready);
	pthread_cond_destroy(&queues[1].write_ready);
	free(queues);
	for(int i = 0; i < filesarrsize; i++){
		sb_destroy(&files[i]);
	}
	free(files);
	pthread_mutex_destroy(&tocompare->lock);
	free(tocompare);
	wfdptr = repo.filehead;
	wfd* wfdtemp;
	while(wfdptr != NULL){
		node* nodeptr = wfdptr->head;
		node* nodetemp;
		while(nodeptr != NULL){
			nodetemp = nodeptr;
			nodeptr = nodeptr->next;
			sb_destroy(&nodetemp->word);
			free(nodetemp);
		}
		wfdtemp = wfdptr;
		wfdptr = wfdptr->next;
		sb_destroy(&wfdtemp->path);
		free(wfdtemp);
	}
	pthread_mutex_destroy(&repo.lock);

	if(status) return EXIT_FAILURE;
	return EXIT_SUCCESS;
}


