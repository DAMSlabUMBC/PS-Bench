/* ri_registry.c */
#include <string.h>
#include <stdlib.h>

#include "ri_registry.h"
#include "mosquitto_internal.h"
#include "util_mosq.h"

/* Linked-list node */
struct ri_entry {
    char *sub_id;
    char **pub_ids_sent_to;
    uint num_pubs_sent_to;
    char *info;
    struct ri_entry *next;
};

static struct ri_entry *ri_head = NULL;

void ri_registry_init(void)
{
    ri_head = NULL;
}

void ri_registry_cleanup(void)
{
    struct ri_entry *cur = ri_head;
    while(cur){
        struct ri_entry *temp = cur;
        cur = cur->next;
        mosquitto_FREE(temp->sub_id);
        mosquitto_FREE(temp->info);

        for(uint i = 0; i < temp->num_pubs_sent_to; i++)
        {
            mosquitto_FREE(temp->pub_ids_sent_to[i]);
        }

        mosquitto_FREE(temp);
    }
    ri_head = NULL;
}

/* If sub_id and topic exists, overwrite its info otherwise create a new entry. */
void ri__register_info(const char *sub_id, const char *info)
{
    struct ri_entry *curr = ri_head;
    while(curr){
        if(!strcmp(curr->sub_id, sub_id)){
            mosquitto_FREE(curr->info);
            curr->info = mosquitto_strdup(info);
            return;
        }
        curr = curr->next;
    }

    /* Not found so allocate a new entry */
    struct ri_entry *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return;
    e->sub_id = mosquitto_strdup(sub_id);
    e->num_pubs_sent_to  = 0;
    e->pub_ids_sent_to = mosquitto_malloc(sizeof(char*));
    e->info   = mosquitto_strdup(info);
    e->next   = ri_head;
    ri_head   = e;
}

bool ri__has_sent_to_pub(const char *pub_id, const char *sub_id)
{
    struct ri_entry *curr = ri_head;
    while(curr){
        if(!strcmp(curr->sub_id, sub_id)){

            for(uint i = 0; i < curr->num_pubs_sent_to; i++)
            {
                if(!strcmp(curr->pub_ids_sent_to[i], pub_id))
                {
                    return true;
                }
            }
            return false;
        }
        curr = curr->next;
    }
    return false;
}

void ri__mark_sent_to_pub(const char *pub_id, const char *sub_id)
{
    struct ri_entry *curr = ri_head;
    while(curr){
        if(!strcmp(curr->sub_id, sub_id)){

            for(uint i = 0; i < curr->num_pubs_sent_to; i++)
            {
                if(!strcmp(curr->pub_ids_sent_to[i], pub_id))
                {
                    return;
                }
            }

            curr->pub_ids_sent_to = mosquitto_realloc(curr->pub_ids_sent_to, sizeof(char*) * (curr->num_pubs_sent_to + 1));
            curr->pub_ids_sent_to[curr->num_pubs_sent_to] = mosquitto_strdup(pub_id);
            curr->num_pubs_sent_to++;

            return;
        }
        curr = curr->next;
    }
}

/* Returns the stored info, or a NULL if not found. */
char *ri__lookup_info(const char *sub_id)
{
    struct ri_entry *curr = ri_head;
    while(curr){
        if(!strcmp(curr->sub_id, sub_id)){
            return curr->info;
        }
        curr = curr->next;
    }
    return NULL;
}
