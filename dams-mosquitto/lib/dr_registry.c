#include <string.h>
#include "dr_registry.h"
#include "mosquitto_internal.h"
#include "util_mosq.h"


struct dr_entry *dr_head = NULL;
struct dr_retained_entry *dr_retained_head = NULL;

void dr_registry_init(void)
{
    dr_head = NULL;
    dr_retained_head = NULL;
}

void dr_registry_cleanup(void)
{
    while(dr_head){
        struct dr_entry *e = dr_head;
        dr_head = dr_head->next;
        mosquitto_FREE(e->pub_id);
        mosquitto_FREE(e->topic);
        while(e->sub_list){
            struct dr_sublist *s = e->sub_list;
            e->sub_list = s->next;
            mosquitto_FREE(s->sub_id);
            mosquitto_FREE(s);
        }
        mosquitto_FREE(e);
    }

    while(dr_retained_head)
    {
        struct dr_retained_entry *e = dr_retained_head;
        dr_retained_head = dr_retained_head->next;
        mosquitto_FREE(e->pub_id);
        mosquitto_FREE(e->topic);
        mosquitto_FREE(e);
    }
}

static struct dr_entry *dr__find_or_create(const char *pub_id, const char *topic)
{
    struct dr_entry *cur = dr_head;
    while(cur){
        if(!strcmp(cur->pub_id, pub_id) && !strcmp(cur->topic, topic)){
            return cur;
        }
        cur = cur->next;
    }
    struct dr_entry *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return NULL;
    e->pub_id = mosquitto_strdup(pub_id);
    e->topic  = mosquitto_strdup(topic);
    e->sub_list = NULL;
    e->next = dr_head;
    dr_head = e;
    return e;
}

void dr__record_recipient(const char *pub_id, const char *topic, const char *sub_id)
{
    struct dr_entry *entry = dr__find_or_create(pub_id, topic);
    if(!entry) return;
    /* Check if sub_id is already in sub_list */
    struct dr_sublist *s = entry->sub_list;
    while(s){
        if(!strcmp(s->sub_id, sub_id)){
            return; /* already stored */
        }
        s = s->next;
    }
    s = mosquitto_calloc(1, sizeof(*s));
    if(!s) return;
    s->sub_id = mosquitto_strdup(sub_id);
    s->next = entry->sub_list;
    entry->sub_list = s;
}

struct dr_sublist *dr__get_recipients(const char *pub_id, const char *topic)
{
    struct dr_entry *cur = dr_head;
    while(cur){
        if(!strcmp(cur->pub_id, pub_id) && !strcmp(cur->topic, topic)){
            /* Build a shallow copy of sub_list so the caller can iterate. */
            struct dr_sublist *copy_head = NULL;
            struct dr_sublist *orig = cur->sub_list;
            while(orig){
                struct dr_sublist *tmp = mosquitto_calloc(1, sizeof(*tmp));
                if(!tmp){
                    dr__free_sublist(copy_head);
                    return NULL;
                }
                tmp->sub_id = mosquitto_strdup(orig->sub_id);
                tmp->next = copy_head;
                copy_head = tmp;
                orig = orig->next;
            }
            return copy_head;
        }
        cur = cur->next;
    }
    return NULL;
}

void dr__record_retained_publisher(const char* pub_id, const char * topic)
{
    struct dr_retained_entry *cur = dr_retained_head;
    while(cur){
        if(!strcmp(cur->topic, topic)){
            mosquitto_FREE(cur->pub_id);
            cur->pub_id = mosquitto_strdup(pub_id);
            return;
        }
        cur = cur->next;
    }

    struct dr_retained_entry *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return;
    e->pub_id = mosquitto_strdup(pub_id);
    e->topic  = mosquitto_strdup(topic);
    e->next = dr_retained_head;
    dr_retained_head = e;
    return;
}

void dr__free_sublist(struct dr_sublist *list)
{
    while(list){
        struct dr_sublist *tmp = list;
        list = list->next;
        mosquitto_FREE(tmp->sub_id);
        mosquitto_FREE(tmp);
    }
}
