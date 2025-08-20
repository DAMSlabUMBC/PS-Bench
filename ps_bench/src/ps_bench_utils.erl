-module(ps_bench_utils).

-include("ps_bench_config.hrl").

%% public
-export([initialize_rng_seed/0, initialize_rng_seed/1, generate_mqtt_payload_data/3,
         generate_dds_datatype_data/2, evaluate_uniform_chance/1, decode_seq_header/1]).

-export([convert_to_atom/1, convert_to_binary/1]).

-export([log_message/1, log_message/2, log_state_change/1, log_state_change/2]).

initialize_rng_seed() ->
    % No seed provided, use the crypto library to generate a 12 digit seed
    SeedValue = crypto:strong_rand_bytes(12),
    crypto:rand_seed(SeedValue),

    % Save seed value for reproducibility
    persistent_term:put({?MODULE, seed}, SeedValue).

initialize_rng_seed(SeedValue) ->
    % Just seed and save
    crypto:rand_seed(SeedValue),
    persistent_term:put({?MODULE, seed}, SeedValue).

generate_mqtt_payload_data(PayloadSizeMean, PayloadSizeVariance, Topic) ->
    % Calculate payload size according to a normal distribution
    FloatSize = rand:normal(PayloadSizeMean, PayloadSizeVariance),
    IntSize = erlang:round(FloatSize),

    % We need to encode some data in the payload, subtracted the fixed content
    % from the payload size, making sure we don't try to generate a negative amount of bytes
    RandomBytesToGen = max(0, IntSize - ?PAYLOAD_HDR_BYTES),
    RandomBytes = crypto:strong_rand_bytes(RandomBytesToGen),

    % Calculate sequence number.
    % NOTE: Time must be appended as 8-bytes at the front of the payload by the
    %       interface used! We can't add time in this function since that will
    %       add processing time for the benchmark to the latency results.
    %       This function intentionally generates a payload 8 bytes too "small" 
    %       to allow for the interface to add the time data
    Seq = ps_bench_store:get_next_seq_id(Topic),
    Payload = <<Seq:64/unsigned, RandomBytes/binary>>,
    Payload.

generate_dds_datatype_data(PayloadSizeMean, PayloadSizeVariance) ->
    % Calculate payload size according to a normal distribution
    FloatSize = rand:normal(PayloadSizeMean, PayloadSizeVariance),
    IntSize = erlang:round(FloatSize),

    % For DDS, the payload is standalone as we can encode the data we need in the IDL types
    RandomBytes = crypto:strong_rand_bytes(IntSize),

    % Calculate sequence number.
    Seq = ps_bench_store:get_next_seq_id(?DDS_TOPIC),
    {Seq, RandomBytes}.

evaluate_uniform_chance(ChanceOfEvent) when 0.0 =< ChanceOfEvent, ChanceOfEvent =< 1.0 ->
    % Get a random value N, 0.0 <= N < 1.0
    RandVal = rand:uniform(),

    % The event happens if the randomly generated number is less than the chance as a percentage
    % We want strict inequality so the event doesn't fire on 0.0 <= 0.0
    % This is fine since rand:uniform cannot generate 1.0, so a chance of 1.0 will always fire
    RandVal < ChanceOfEvent.

decode_seq_header(<<TimeNs:64/unsigned, Seq:64/unsigned, Rest/binary>>) ->
    {Seq, TimeNs, Rest};

decode_seq_header(Bin) ->
    {undefined, undefined, Bin}.

convert_to_atom(Name) ->
    case Name of
        A when is_atom(A)   -> A;
        B when is_binary(B) -> list_to_atom(binary_to_list(B));
        L when is_list(L)   -> list_to_atom(L)
    end.

convert_to_binary(Name) ->
    case Name of
        B when is_binary(B) -> B;
        A when is_atom(A)   -> list_to_binary(atom_to_list(A));
        L when is_list(L)   -> list_to_binary(L)
    end.

% Logging functions, may move these to a better logger class in the future
log_message(Message) ->
    log_message(Message, []).

log_message(Message, Args) ->
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    FormattedMessage = io_lib:format(Message, Args),
    io:format("[~p] ~s~n", [NodeName, FormattedMessage]).

log_state_change(Message) ->
    log_state_change(Message, []).

log_state_change(Message, Args) ->
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    FormattedMessage = io_lib:format(Message, Args),
    io:format("[~p] === ~s ===~n", [NodeName, FormattedMessage]).