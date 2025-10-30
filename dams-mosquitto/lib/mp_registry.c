/* mp_registry.c */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "mosquitto_internal.h"  
#include "util_mosq.h"                
#include "mp_registry.h"

#define MPREG_HASH_SIZE 256

/* A single entry in the hash chain. */
struct mp_entry {
    char *topic;           /* "sensors/temp" */
    char *purpose_filter;  /* "ads/targeted" */
    struct mp_entry *next; /* pointer to next in the chain */
};

/* The bucket array is for our hash table. */
static struct mp_entry *g_mp_buckets[MPREG_HASH_SIZE];


static unsigned int mp__hashstr(const char *str)
{
    unsigned long hash = 5381; 
    unsigned long c;
    while((c = (unsigned char) *str++)){
        hash = ((hash << 5) + hash) ^ c;
    }
    return (unsigned int)(hash % MPREG_HASH_SIZE);
}

void mp_registry_init(void)
{
    /* Zero out the bucket array  */
    memset(g_mp_buckets, 0, sizeof(g_mp_buckets));
}

void mp_registry_cleanup(void)
{
    /* Free all the entries in all buckets. */
    for(int i=0; i<MPREG_HASH_SIZE; i++){
        struct mp_entry *curr = g_mp_buckets[i];
        while(curr){
            struct mp_entry *tmp = curr;
            curr = curr->next;
            mosquitto_FREE(tmp->topic);
            mosquitto_FREE(tmp->purpose_filter);
            mosquitto_FREE(tmp);
        }
        g_mp_buckets[i] = NULL;
    }
}

/* Store or overwrite a purpose filter for a given topic */
void mp__register_topic(const char* id, const char *topic, const char *mp_value)
{
    char* hash_string = mosquitto_malloc(strlen(id) + strlen(topic) + 1);
    strcpy(hash_string, id);
    strcat(hash_string, topic);
    unsigned int bucket_index = mp__hashstr(hash_string);
    mosquitto_FREE(hash_string);

    /* Search this chain for an existing entry with the same topic */
    struct mp_entry *curr = g_mp_buckets[bucket_index];
    while(curr){
        if(!strcmp(curr->topic, topic)){
            /* Found existing so overwrite the purpose filter */
            mosquitto_FREE(curr->purpose_filter);
            curr->purpose_filter = mosquitto_strdup(mp_value);
            return; 
        }
        curr = curr->next;
    }

    /* Not found so create a new entry and link to head of chain */
    struct mp_entry *entry = mosquitto_calloc(1, sizeof(*entry));
    entry->topic          = mosquitto_strdup(topic);
    entry->purpose_filter = mosquitto_strdup(mp_value);
    entry->next           = g_mp_buckets[bucket_index];
    g_mp_buckets[bucket_index] = entry;
}

/* Look up the purpose filter for a given topic */
char *mp__lookup_topic(const char* id, const char *topic)
{
    char* hash_string = mosquitto_malloc(strlen(id) + strlen(topic) + 1);
    strcpy(hash_string, id);
    strcat(hash_string, topic);
    unsigned int bucket_index = mp__hashstr(hash_string);
    mosquitto_FREE(hash_string);

    struct mp_entry *curr = g_mp_buckets[bucket_index];

    while(curr){
        if(!strcmp(curr->topic, topic)){
            return curr->purpose_filter; /* Found it */
        }
        curr = curr->next;
    }
    return NULL; /* Not found */
}
