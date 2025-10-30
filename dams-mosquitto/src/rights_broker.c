#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include "rights_broker.h"
#include "mosquitto_broker_internal.h" 
#include "util_mosq.h"
#include "property_mosq.h"
#include "send_mosq.h"
#include "dr_registry.h" 

/* Finds a client context by ID by calling db__find_context_by_id(). */
struct mosquitto *broker_find_context_by_id(const char *client_id)
{
    return db__find_context_by_id(client_id);
}

/* Checks if a subscriber is online (active state). */
bool is_sub_online(const char *sub_id)
{
    struct mosquitto *ctx = broker_find_context_by_id(sub_id);
    if(!ctx) return false;
    return (ctx->state == mosq_cs_active);
}

/* Removes Wills or retained messages invoking erasure. */
void handle_remove_stored_messages(const char *publisher_id)
{
    /* Remove will message for publisher*/
    struct mosquitto *pub_ctx = broker_find_context_by_id(publisher_id);
    if(pub_ctx && pub_ctx->will){
        mosquitto_FREE(pub_ctx->will->msg.topic);
        mosquitto_FREE(pub_ctx->will->msg.payload);
        mosquitto_FREE(pub_ctx->will);
        pub_ctx->will = NULL;
    }

    /* Remove retained message for publisher */
    extern struct dr_retained_entry *dr_retained_head;
    struct dr_retained_entry *cur = dr_retained_head;
    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            mosquitto_persist_retain_msg_delete(cur->topic);
        }
        cur = cur->next;
    }
}

/* Sends a basic status response to RNP/<publisher_id>. */
void broker_send_response_success(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *payload, char* response_topic)
{
    if(!publisher_id) return;

    if (response_topic == NULL)
    {
        char onp_topic[256];
        snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);
        response_topic = mosquitto_strdup(onp_topic);
    }

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, mosquitto_strdup(operation));

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Success");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_REASON_KEY, "");
    
    if(corr_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, corr_data, correlation_data_len);
    }

    if(payload)
    {
        db__messages_easy_queue_with_purpose(NULL, response_topic, MOSQ_DAP_OP_PURPOSE, 0, strlen(payload), payload, false, 0, &props);
    }
    else
    {
        db__messages_easy_queue_with_purpose(NULL, response_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
    }

    mosquitto_property_free_all(&props);
}

/* Notifies publisher of offline subs, setting DAP-Deadline & DAP-UnreachedClients. */
void broker_send_response_pending(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, int deadline_sec)
{
    if(!publisher_id) return;
    char onp_topic[256];
    snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, mosquitto_strdup(operation));

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Pending");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_REASON_KEY, "Subscriber not connected");

    char deadline_buf[32];
    snprintf(deadline_buf, sizeof(deadline_buf), "%d", deadline_sec);
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_DEADLINE_KEY, deadline_buf);

    if(corr_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, corr_data, correlation_data_len);
    }

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Sends a final failure to RNP/<publisher_id> (e.g. unknown right or offline never reconnected). */
void broker_send_response_failure(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *reason,
    struct subscriber_list *unreached_subs)
{
    if(!publisher_id) return;
    char onp_topic[256];
    snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, mosquitto_strdup(operation));

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Failure");

    if(reason){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_REASON_KEY, reason);
    }

    if(corr_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, corr_data, correlation_data_len);
    }

    if(unreached_subs)
    {
        char contact_buf[256];
        contact_buf[0] = '\0';
        while(unreached_subs){
            strncat(contact_buf, unreached_subs->sub_id, sizeof(contact_buf)-1);
            strncat(contact_buf, " ", sizeof(contact_buf)-1);
            unreached_subs = unreached_subs->next;
        }
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_UNREACHED_CLIENTS_KEY, contact_buf);
    }

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Finds (sub_id, topic) for all subscriptions that got data from publisher_id. */
struct subscription_list *find_subscriptions_for_publisher(const char *publisher_id)
{
    struct subscription_list *head = NULL;
    struct subscription_list *check_ptr = NULL;
    extern struct dr_entry *dr_head;
    struct dr_entry *cur = dr_head;

    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            struct dr_sublist *s = cur->sub_list;
            while(s){

                bool found = false;
                check_ptr = head;
                while(check_ptr)
                {
                    /* We've already added this subscriber */
                    if(!strcmp(check_ptr->subscriber_id, s->sub_id))
                    {
                        found = true;
                        break;
                    }

                    check_ptr = check_ptr->next;
                }

                if (!found) 
                {
                    struct subscription_list *node = mosquitto_calloc(1, sizeof(*node));
                    if(!node) return head;
                    node->subscriber_id = mosquitto_strdup(s->sub_id);
                    node->topic        = mosquitto_strdup(cur->topic);
                    node->next         = head;
                    head = node;
                }

                s = s->next;
            }
        }
        cur = cur->next;
    }
    return head;
}

/* Finds subs with matching topic if data_filter is in the topic name. */
struct subscriber_list *find_subscribers_with_data(const char *publisher_id, const char *data_filter)
{
    struct subscriber_list *head = NULL;
    struct subscriber_list *check_ptr = NULL;
    extern struct dr_entry *dr_head;
    struct dr_entry *cur = dr_head;

    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            if(data_filter && (strstr(cur->topic, data_filter) || !strcmp(data_filter, MOSQ_DAP_ALLOW_ALL_FILTER))) {
                struct dr_sublist *s = cur->sub_list;
                while(s){

                    bool found = false;
                    check_ptr = head;
                    while(check_ptr)
                    {
                        /* We've already added this subscriber */
                        if(!strcmp(check_ptr->sub_id, s->sub_id))
                        {
                            found = true;
                            break;
                        }

                        check_ptr = check_ptr->next;
                    }

                    if (!found) 
                    {
                        /* New subscriber, add */
                        struct subscriber_list *node = mosquitto_calloc(1, sizeof(*node));
                        if(!node) return head;
                        node->sub_id = mosquitto_strdup(s->sub_id);
                        node->next   = head;
                        head         = node;
                    }
                    s = s->next;
                }
            }
        }
        cur = cur->next;
    }
    return head;
}

/* Publishes a right request to RRS/<sub> if online, else collects them offline. */
struct subscriber_list *forward_request_to_connected(struct subscriber_list *sub_list, struct mosquitto_base_msg *msg_data, char* response_topic, char* op_id, char* op_info, char* correlation_data, uint16_t correlation_data_len)
{
    struct subscriber_list *offline_head = NULL;

    while(sub_list){
        if(is_sub_online(sub_list->sub_id)){
            char ors_topic[256];
            snprintf(ors_topic, sizeof(ors_topic), "%s/%s", MOSQ_DAP_TOPIC_ORS, sub_list->sub_id);
            response_topic = ors_topic;

            mosquitto_property *props = NULL;
            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_CONSENT_KEY, "1");

            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_ID_KEY, mosquitto_strdup(msg_data->source_id));

            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_OP_KEY, mosquitto_strdup(op_id));

            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_OP_INFO_KEY, mosquitto_strdup(op_info));

            if(correlation_data){
                mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, correlation_data, correlation_data_len);
            }

            db__messages_easy_queue_with_purpose(NULL, mosquitto_strdup(response_topic), MOSQ_DAP_OP_PURPOSE, msg_data->qos, msg_data->payloadlen, msg_data->payload, msg_data->retain, msg_data->expiry_time, &props);
        } else {
            struct subscriber_list *off = mosquitto_calloc(1, sizeof(*off));
            off->sub_id = mosquitto_strdup(sub_list->sub_id);
            off->next   = offline_head;
            offline_head= off;
        }
        sub_list = sub_list->next;
    }
    return offline_head;
}