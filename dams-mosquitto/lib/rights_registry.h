#ifndef RIGHTS_REGISTRY_H
#define RIGHTS_REGISTRY_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A single GDPR right-invocation request in MQTT-DAP. */
struct right_invocation {
    char *client_id;
    char *correlation_data;
    char *invoked_right;
    char *data_filter;
    char *gdpr_reason;
    bool completed;

    struct right_invocation *next; /* pointer to next in singly-linked list */
};

/* Initialize the internal data structures for storing requests. */
void rights_registry_init(void);

/* Clean up all stored requests. */
void rights_registry_cleanup(void);

/* Stores a new right-invocation request, keyed by client_id and correlation_data */
int rr_store_request(const char *client_id,
                     const char *correlation_data,
                     const char *invoked_right,
                     const char *data_filter);

/*Looks up an existing request by client_id and the correlation_data.*/
struct right_invocation* rr_lookup_request(const char *client_id,
                                           const char *correlation_data);

/*
 * Mark a request as completed but a reason string can be stored in gdpr_reason.
 * If 'success' is false but 'reason' is set then a decline is implied with an explanation
 */
int rr_set_completed(const char *client_id,
                     const char *correlation_data,
                     const char *reason);

#ifdef __cplusplus
}
#endif

#endif /* RIGHTS_REGISTRY_H */
