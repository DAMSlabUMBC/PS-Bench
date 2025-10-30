/* sp_registry.c */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "sp_registry.h"

#define SPREG_HASH_SIZE 256

/* Chain-based hash entry */
struct sp_entry {
    char *topic;
    char *purpose_sp;
    struct sp_entry *next;
};

/* The array for our hash table. */
static struct sp_entry *g_sp_buckets[SPREG_HASH_SIZE];

/* Simple hash */
static unsigned int sp__hashstr(const char *str)
{
    unsigned long hash = 5381;
    uint32_t c;
    while((c = (unsigned char)(*str++))){
        hash = ((hash << 5) + hash) ^ c;
    }
    return (unsigned int)(hash % SPREG_HASH_SIZE);
}

void sp_registry_init(void)
{
    memset(g_sp_buckets, 0, sizeof(g_sp_buckets));
}

void sp_registry_cleanup(void)
{
    for(int i = 0; i < SPREG_HASH_SIZE; i++){
        struct sp_entry *curr = g_sp_buckets[i];
        while(curr){
            struct sp_entry *tmp = curr;
            curr = curr->next;
            mosquitto_FREE(tmp->topic);
            mosquitto_FREE(tmp->purpose_sp);
            mosquitto_FREE(tmp);
        }
        g_sp_buckets[i] = NULL;
    }
}

/* Store or overwrite SP for a topic */
void sp__register_topic(const char* id, const char *topic, const char *sp_value)
{
    char* hash_string = mosquitto_malloc(strlen(id) + strlen(topic) + 1);
    strcpy(hash_string, id);
    strcat(hash_string, topic);
    unsigned int idx = sp__hashstr(hash_string);
    mosquitto_FREE(hash_string);

    struct sp_entry *curr = g_sp_buckets[idx];
    while(curr){
        if(!strcmp(curr->topic, topic)){
            mosquitto_FREE(curr->purpose_sp);
            curr->purpose_sp = mosquitto_strdup(sp_value);
            return;
        }
        curr = curr->next;
    }
    struct sp_entry *entry = mosquitto_calloc(1, sizeof(*entry));
    entry->topic      = mosquitto_strdup(topic);
    entry->purpose_sp = mosquitto_strdup(sp_value);
    entry->next       = g_sp_buckets[idx];
    g_sp_buckets[idx] = entry;
}

/* Look up the purpose filter for a given topic */
char *sp__lookup_topic(const char* id, const char *topic)
{
    char* hash_string = mosquitto_malloc(strlen(id) + strlen(topic) + 1);
    strcpy(hash_string, id);
    strcat(hash_string, topic);
    unsigned int idx = sp__hashstr(hash_string);
    mosquitto_FREE(hash_string);

    struct sp_entry *curr = g_sp_buckets[idx];
    while(curr){
        if(!strcmp(curr->topic, topic)){
            return curr->purpose_sp;
        }
        curr = curr->next;
    }
    return NULL;
}
