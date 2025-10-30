#ifndef SP_REGISTRY_H
#define SP_REGISTRY_H

#ifdef __cplusplus
extern "C" {
#endif

void sp_registry_init(void);
void sp_registry_cleanup(void);
void sp__register_topic(const char* id, const char *topic, const char *sp_value);
char *sp__lookup_topic(const char* id, const char *topic);

#ifdef __cplusplus
}
#endif

#endif
