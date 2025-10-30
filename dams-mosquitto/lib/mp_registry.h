#ifndef MP_REGISTRY_H
#define MP_REGISTRY_H

#ifdef __cplusplus
extern "C" {
#endif

/* Initializes the MP registry */
void mp_registry_init(void);

/* Cleans up all the stored entries */
void mp_registry_cleanup(void);

/* Registers or overwrites a purpose filter for the given topic */
void mp__register_topic(const char* id, const char *topic, const char *mp_value);

/* Looks up the stored purpose filter for a given topic. */

char *mp__lookup_topic(const char* id, const char *topic);

#ifdef __cplusplus
}
#endif

#endif 
