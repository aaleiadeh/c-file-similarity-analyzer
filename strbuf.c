#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "strbuf.h"
//Please note I have decided that the string terminator will be at the end of the 'used' portion always
//If the user decides to insert a character that will create a gap of undetermined values in btwn when reallocating
//the previous null terminator will not be overwritten and a new one will be implemented into the new 'used' position
int sb_init(strbuf_t *buff, size_t length)
{
	buff->data = malloc(sizeof(char) * length);
	if(!buff->data) return 1;
	buff->used = 0;
	buff->length = length;
	buff->data[buff->used] = 0;
	return 0;
}

void sb_destroy(strbuf_t *buff)
{
	free(buff->data);
}

void sb_reset(strbuf_t *buff)
{
	buff->used = 0;
	buff->data[0] = 0;
}

void sb_removen(strbuf_t *buff, int length)
{
	buff->used -= length;
	buff->data[buff->used] = 0;
}

int sb_append(strbuf_t *buff, char item)
{
	if(buff->used == buff->length-1)//-1 Because we need to reserve a space for the null terminator
	{
		size_t size = buff->length * 2;
		char *p = realloc(buff->data, sizeof(char) * size);
		if(!p) return 1;
		buff->data = p;
		buff->length = size;
	}
	buff->data[buff->used] = item;
	++buff->used;
	buff->data[buff->used] = 0;
	return 0;
}

int sb_remove(strbuf_t *buff, char *item)
{
	if(buff->used == 0) return 1;
	--buff->used;
	if(item) *item = buff->data[buff->used];
	buff->data[buff->used] = 0;
	return 0;
}

//Checlist
//Append Case
//2 Scenarios of Mem Reallocation: Is doubling or up to index greater
//Data handling scenarios: In the middle of used portion or to the right of used portion
int sb_insert(strbuf_t *buff, int index, char item)
{
	if(index == buff->used)
	{
		return sb_append(buff, item);
	}
	if(buff->used == buff->length-1)
	{
		size_t size = buff->length * 2;
		char *p = realloc(buff->data, sizeof(char) * size);
		if(!p) return 1;
		buff->data = p;
		buff->length = size;
	}
	if(index >= buff->length)
	{
		size_t size = buff->length;
		if(size * 2 >= index) size = size * 2; else size = index + 2; //We add 2. 1 To change index into size
		char *p = realloc(buff->data, sizeof(char) * size);	      //and 1 to make space for terminator
		if(!p) return 1;
		buff->data = p;
		buff->length = size;
	}

	if(index > buff->used)
	{
		buff->data[index] = item;
		buff->used = index + 1;
		buff->data[buff->used] = 0;
		return 0;
	}
	else //Everything to the right 1
	{
		int i;
		for(i = buff->used; i >= index; i--)
		{
			buff->data[i+1] = buff->data[i];
		}
		buff->data[index] = item;
		++buff->used;
		return 0;
	}
}

int sb_concat(strbuf_t *sb, char *str)
{
	int len = strlen(str);
	if(len == 0) return 0;

	if((sb->used + len) >= sb->length)
	{
		size_t size = sb->length + len;
		char *p = realloc(sb->data, sizeof(char) * size);
		if(!p) return 1;
		sb->data = p;
		sb->length = size;
	}

	int i;
	for(i = 0; i < len; i++)
	{
		sb->data[sb->used] = str[i];
		++sb->used;
	}
	sb->data[sb->used] = 0;
	return 0;
}

/*int main(int argc, char *argv[])
{
	strbuf_t buff;
	sb_init(&buff, 7);
	for(int i = 0; i < 6; i++)
	{
		sb_append(&buff, i+65);
	}
	sb_concat(&buff, "Hello");
	sb_append(&buff, 'x');
	sb_remove(&buff, NULL);
	for(int i = 0; i < buff.length; i++)
	{
		printf("%c Pos: %d\n", buff.data[i], i);
	}
	printf("%s\n", buff.data);
	printf("%ld %ld", buff.used, buff.length);
	return 0;
}*/
