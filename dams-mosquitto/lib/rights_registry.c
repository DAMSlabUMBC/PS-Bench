#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "rights_registry.h"
#include "mosquitto_internal.h"  
#include "util_mosq.h"           

/* Head of a simple singly-linked list and each node is a struct right_invocation. */
static struct right_invocation *g_requests_head = NULL;

/* Initialize the registry by resetting the head pointer. */
void rights_registry_init(void)
{
    g_requests_head = NULL;
}

/* Frees all requests in the list. */
void rights_registry_cleanup(void)
{
    struct right_invocation *current = g_requests_head;
    while(current){
        struct right_invocation *tmp = current;
        current = current->next;

        mosquitto_FREE(tmp->client_id);
        mosquitto_FREE(tmp->correlation_data);
        mosquitto_FREE(tmp->invoked_right);
        mosquitto_FREE(tmp->data_filter);
        mosquitto_FREE(tmp->gdpr_reason);
        mosquitto_FREE(tmp);
    }
    g_requests_head = NULL;
}

/*
 * Insert a new GDPR right invocation. If the exact client_id and correlation_data is already present then it is overwritten
 */
int rr_store_request(const char *client_id,
                     const char *correlation_data,
                     const char *invoked_right,
                     const char *data_filter)
{
    /* Check if this request is already stored. */
    struct right_invocation *iter = g_requests_head;
    while(iter){
         /* Match on both client_id and correlation_data */
        if(iter->client_id && !strcmp(iter->client_id, client_id) &&
          iter->correlation_data && !strcmp(iter->correlation_data, correlation_data))
        {
            /* Overwrite fields in the existing record. */

            /* Free old invoked_right if it exists */
            mosquitto_FREE(iter->invoked_right);
            if(invoked_right){
                iter->invoked_right = mosquitto_strdup(invoked_right);
                if(!iter->invoked_right){
                  return 1; /* out of memory */
                }
            } else {
              iter->invoked_right = NULL;
            }

            /* Free old data_filter if it exists */
            mosquitto_FREE(iter->data_filter);
            if(data_filter){
                iter->data_filter = mosquitto_strdup(data_filter);
                if(!iter->data_filter){
                  return 1; /* out of memory */
                }
            } else {
                iter->data_filter = NULL;
            }

            /* Reset status on overwrite: */
            iter->completed = false;
            mosquitto_FREE(iter->gdpr_reason);
            iter->gdpr_reason = NULL;

            return 0; /* success */
        }  
        iter = iter->next;
    }

    /* Allocate a new node. */
    struct right_invocation *req = mosquitto_calloc(1, sizeof(*req));
    if(!req){
        return 1; /* out of memory */
    }

    /* Duplicate strings if they exist. */
    if(client_id){
        req->client_id = mosquitto_strdup(client_id);
        if(!req->client_id) goto fail;
    }
    if(correlation_data){
        req->correlation_data = mosquitto_strdup(correlation_data);
        if(!req->correlation_data) goto fail;
    }
    if(invoked_right){
        req->invoked_right = mosquitto_strdup(invoked_right);
        if(!req->invoked_right) goto fail;
    }
    if(data_filter){
        req->data_filter = mosquitto_strdup(data_filter);
        if(!req->data_filter) goto fail;
    }
    /* gdpr_reason will stay NULL until used and completed is false by default. */

    /* Link the new node at the front of the list. */
    req->next = g_requests_head;
    g_requests_head = req;

    return 0; /* success */

fail:
    mosquitto_FREE(req->client_id);
    mosquitto_FREE(req->correlation_data);
    mosquitto_FREE(req->invoked_right);
    mosquitto_FREE(req->data_filter);
    mosquitto_FREE(req);
    return 1; /* error */
}

/* Find an existing invocation by client_id and correlation_data. */
struct right_invocation* rr_lookup_request(const char *client_id,
                                           const char *correlation_data)
{
    struct right_invocation *iter = g_requests_head;
    while(iter){
        if(iter->client_id && !strcmp(iter->client_id, client_id) &&
           iter->correlation_data && !strcmp(iter->correlation_data, correlation_data))
        {
            return iter;
        }
        iter = iter->next;
    }
    return NULL; 
}

/* Mark a request as completed fuffiled or declined and optionally attach a reason string . */
int rr_set_completed(const char *client_id,
                     const char *correlation_data,
                     const char *reason)
{
    struct right_invocation *req = rr_lookup_request(client_id, correlation_data);
    if(!req){
        return 1; /* No matching request found. */
    }
    req->completed = true;

    if(reason){
        /* May want to free an old reason first if it existed. */
        if(req->gdpr_reason){
            mosquitto_FREE(req->gdpr_reason);
            req->gdpr_reason = NULL;
        }
        req->gdpr_reason = mosquitto_strdup(reason);
    }

    /* If success is false but reason is set then a declined request. */
    return 0;
}
