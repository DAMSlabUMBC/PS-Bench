#ifndef RIGHTS_BROKER_H
#define RIGHTS_BROKER_H

#include <stdbool.h>

/* Forward-declare mosquitto struct if needed. */
struct mosquitto;
struct subscriber_list;  
struct mosquitto_base_msg;  
struct subscription_list;

/* A function to lookup a client context by ID and a function to check if a subscriber is online. */
struct mosquitto *broker_find_context_by_id(const char *client_id);
bool is_sub_online(const char *sub_id);

/* Removes Will or retained messages. */
void handle_remove_stored_messages(const char *publisher_id);

/* Responses back to a publisher on RNP/<publisher_id>. */
void broker_send_response_success(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *payload, char* response_topic);
void broker_send_response_pending(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, int deadline_sec);
void broker_send_response_failure(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *reason,
    struct subscriber_list *unreached_subs);

/* For enumerating who got the publisher's data (C1). */
struct subscription_list *find_subscriptions_for_publisher(const char *publisher_id);

/* For enumerating who has data matching a filter (C2/C3). */
struct subscriber_list *find_subscribers_with_data(const char *publisher_id, const char *data_filter);

/* Forward a right request to RRS/<sub_id> if online, else add to an offline list. */
struct subscriber_list *forward_request_to_connected(struct subscriber_list *sub_list, struct mosquitto_base_msg *msg_data, char* response_topic, char* op_id, char* op_info, char* correlation_data, uint16_t correlation_data_len);

/* Data structures for returning lists of subscribers. */
typedef struct subscription_list {
    char *subscriber_id;
    char *topic;
    struct subscription_list *next;
} subscription_list;

typedef struct subscriber_list {
    char *sub_id;
    struct subscriber_list *next;
} subscriber_list;

#endif
