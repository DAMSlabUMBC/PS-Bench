#include "util_mosq.h"

#include "purpose_filters.h"

uint32_t parse_one_level(const char *level, char ***out_terms)
{
    *out_terms = NULL;
    uint32_t term_count = 0;

    size_t len = strlen(level);
    if (len >= 2 && level[0] == '{' && level[len - 1] == '}')
    {
        char *inside = strndup(level + 1, len - 2);
        char *saveptr = NULL;
        char *token = strtok_r(inside, ",", &saveptr);
        while (token)
        {
            term_count++;
            *out_terms = mosquitto_realloc(*out_terms, term_count * sizeof(char*));
            (*out_terms)[term_count - 1] = strdup(token);
            token = strtok_r(NULL, ",", &saveptr);
        }
        mosquitto_FREE(inside);
    }
    else
    {
        term_count = 1;
        *out_terms = mosquitto_malloc(sizeof(char*));
        (*out_terms)[0] = strdup(level);
    }
    return term_count;
}

mosquitto_pf_expansion *combine_expansions(mosquitto_pf_expansion *old_list, uint32_t old_count,
                                       char **terms, uint32_t term_count,
                                       uint32_t *out_count)
{
    mosquitto_pf_expansion *temp = NULL;
    uint32_t temp_count = 0;

    for(uint32_t i = 0; i < old_count; i++)
    {
        if(old_list[i].ended)
        {
            temp_count++;
            temp = mosquitto_realloc(temp, temp_count * sizeof(mosquitto_pf_expansion));
            temp[temp_count - 1].str = strdup(old_list[i].str);
            temp[temp_count - 1].ended = true;
            continue;
        }

        for(uint32_t j = 0; j < term_count; j++)
        {
            const char *t = terms[j];
            if(strcmp(t, ".") == 0)
            {
                temp_count++;
                temp = mosquitto_realloc(temp, temp_count * sizeof(mosquitto_pf_expansion));
                temp[temp_count - 1].str = strdup(old_list[i].str);
                temp[temp_count - 1].ended = true;
            }
            else
            {
                size_t old_len = strlen(old_list[i].str);
                if(old_len == 0)
                {
                    temp_count++;
                    temp = mosquitto_realloc(temp, temp_count * sizeof(mosquitto_pf_expansion));
                    temp[temp_count - 1].str = strdup(t);
                    temp[temp_count - 1].ended = false;
                }
                else
                {
                    size_t new_len = old_len + 1 + strlen(t) + 1;
                    char *buf = mosquitto_malloc(new_len);
                    snprintf(buf, new_len, "%s/%s", old_list[i].str, t);
                    temp_count++;
                    temp = mosquitto_realloc(temp, temp_count * sizeof(mosquitto_pf_expansion));
                    temp[temp_count - 1].str = buf;
                    temp[temp_count - 1].ended = false;
                }
            }
        }
    }

    *out_count = temp_count;
    return temp;
}

char **parse_purpose_filter(const char *filter, uint32_t *num_results)
{
    mosquitto_pf_expansion *exp_list = mosquitto_malloc(sizeof(mosquitto_pf_expansion));
    exp_list[0].str = strdup("");
    exp_list[0].ended = false;
    uint32_t exp_count = 1;

    char *copy = strdup(filter);
    char *saveptr;
    char *level = strtok_r(copy, "/", &saveptr);

    while(level)
    {
        char **terms = NULL;
        uint32_t tcount = parse_one_level(level, &terms);
        uint32_t new_count = 0;
        mosquitto_pf_expansion *new_list = combine_expansions(exp_list, exp_count,
                                                   terms, tcount, &new_count);

        for(uint32_t i = 0; i < tcount; i++)
        {
            mosquitto_FREE(terms[i]);
        }
        mosquitto_FREE(terms);

        exp_list = new_list;
        exp_count = new_count;

        level = strtok_r(NULL, "/", &saveptr);
    }

    mosquitto_FREE(copy);

    char **results = mosquitto_malloc(exp_count * sizeof(char*));
    for(uint32_t i = 0; i < exp_count; i++)
    {
        results[i] = exp_list[i].str;
    }
    mosquitto_FREE(exp_list);

    *num_results = exp_count;
    return results;
}