/* ri_registry.h */
#ifndef RI_REGISTRY_H
#define RI_REGISTRY_H

#include "util_mosq.h"

/* Initializes the registry. */
void ri_registry_init(void);

/* Frees all stored entries. */
void ri_registry_cleanup(void);

/* Stores or overwrites info for the sub_id and topic. */
void ri__register_info(const char *sub_id, const char *info);

/* Checks if we've sent the information to the publisher */
bool ri__has_sent_to_pub(const char *pub_id, const char *sub_id);

/* Marks that we've sent the information to the publish */
void ri__mark_sent_to_pub(const char *pub_id, const char *sub_id);

/* Retrieves info for sub_id and topic, or returns NULL if none. */
char *ri__lookup_info(const char *sub_id);

#endif
