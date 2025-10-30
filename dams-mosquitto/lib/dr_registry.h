/* dr_registry.h */
#ifndef DR_REGISTRY_H
#define DR_REGISTRY_H

struct dr_sublist {
    char *sub_id;
    struct dr_sublist *next;
};

struct dr_entry {
    char *pub_id;
    char *topic;
    struct dr_sublist *sub_list;
    struct dr_entry *next;
};

struct dr_retained_entry {
    char *pub_id;
    char *topic;
    struct dr_retained_entry *next;
};

extern struct dr_entry *dr_head; 
extern struct dr_retained_entry *dr_retained_head; 

void dr_registry_init(void);
void dr_registry_cleanup(void);

/* Add a subscriber to pub_id and topic. */
void dr__record_recipient(const char *pub_id, const char *topic, const char *sub_id);
void dr__record_retained_publisher(const char *pub_id, const char *topic);


/* Freed after use. */
struct dr_sublist *dr__get_recipients(const char *pub_id, const char *topic);
void dr__free_sublist(struct dr_sublist *list);

#endif
