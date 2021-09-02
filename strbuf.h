typedef struct {
	size_t length;
	size_t used;
	char *data;
} strbuf_t;

int sb_init(strbuf_t *, size_t);
void sb_destroy(strbuf_t *);
void sb_reset(strbuf_t *);
void sb_removen(strbuf_t *, int);
int sb_append(strbuf_t *, char);
int sb_remove(strbuf_t *, char *);
int sb_insert(strbuf_t *, int, char);
int sb_concat(strbuf_t *, char *);
