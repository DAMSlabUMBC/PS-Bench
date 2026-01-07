-module(ps_bench_utils).

-include("ps_bench_config.hrl").

%% public
-export([initialize_rng_seed/0, initialize_rng_seed/1, generate_mqtt_payload_data/3,
         generate_dds_datatype_data/2, evaluate_uniform_chance/1, decode_seq_header/1, 
         fetch_rng_seed/0, decode_seq_header_with_publisher/1]).

-export([convert_to_atom/1, convert_to_binary/1, convert_to_list/1]).

-export([log_message/1, log_message/2, log_state_change/1, log_state_change/2]).

% TODO, Fix RNG seeding
initialize_rng_seed() ->
    % No seed provided, use the crypto library to generate a 12 digit seed
    Seed = try_crypto_seed(2000),       
    rand:seed(exsplus, Seed),
    persistent_term:put({?MODULE, seed}, Seed),
    Seed.

initialize_rng_seed(SeedArg) ->
    % Just seed and save
     Seed =
        case SeedArg of
            {A,B,C} when is_integer(A), is_integer(B), is_integer(C) ->
                {A,B,C};
            <<A:32,B:32,C:32,_/binary>> ->  %% 12 or 16 bytes
                {A,B,C};
            I when is_integer(I) ->
                mix_seed(I);
            Bin when is_binary(Bin) ->
                mix_seed(erlang:phash2(Bin))
        end,
    rand:seed(exsplus, Seed),
    persistent_term:put({?MODULE, seed}, Seed),
    Seed.

fetch_rng_seed() ->
    Seed = persistent_term:get({?MODULE, seed}, undefined),
    case Seed of 
        undefined -> 
            % Create new seed if not already existing
            initialize_rng_seed();
        _ -> 
            Seed
    end.

try_crypto_seed(TimeoutMs) ->
    Parent = self(),
    Ref = make_ref(),
    _Pid = spawn(fun() ->
        %% 16 bytes -> 3x32-bit parts (+ 32-bit spare)
        Bytes = crypto:strong_rand_bytes(16),
        <<A:32,B:32,C:32,_/binary>> = Bytes,
        Parent ! {Ref, {A,B,C}}
    end),
    receive
        {Ref, Seed = {_,_,_}} -> Seed
    after TimeoutMs ->
        %% Fallback: never block startup
        make_time_seed()
    end.

make_time_seed() ->
    T = erlang:system_time(),
    U = erlang:unique_integer([positive]),
    N = erlang:phash2(node()),
    {T band 16#3fffffff, U band 16#3fffffff, N band 16#3fffffff}.

mix_seed(Extra) ->
    T = erlang:system_time() bxor Extra,
    U = erlang:unique_integer([positive]) bxor (Extra bsl 13),
    N = erlang:phash2(node()) bxor (Extra bsl 7),
    {T band 16#3fffffff, U band 16#3fffffff, N band 16#3fffffff}.

generate_mqtt_payload_data(PayloadSizeMean, PayloadSizeVariance, Topic) ->
    % Calculate payload size according to a normal distribution
    FloatSize = rand:normal(PayloadSizeMean, PayloadSizeVariance),
    IntSize = erlang:round(FloatSize),

    % Get publisher ID (node name for now)
    {ok, PublisherID} = ps_bench_config_manager:fetch_node_name(),
    PublisherBin = atom_to_binary(PublisherID, utf8),
    PublisherSize = byte_size(PublisherBin),

    % We need to encode some data in the payload, subtracted the fixed content
    % from the payload size, making sure we don't try to generate a negative amount of bytes
    OurHeaderSize = 8 + 2 + PublisherSize,
    TimeHeaderSize = 8,
    RandomBytesToGen = max(0, IntSize - OurHeaderSize - TimeHeaderSize),
    RandomBytes = crypto:strong_rand_bytes(RandomBytesToGen),

    % Calculate sequence number.
    % NOTE: Time must be appended as 8-bytes at the front of the payload by the
    %       interface used! We can't add time in this function since that will
    %       add processing time for the benchmark to the latency results.
    %       This function intentionally generates a payload 8 bytes too "small" 
    %       to allow for the interface to add the time data
    Seq = ps_bench_store:get_next_seq_id(Topic),
    Payload = <<Seq:64/unsigned, PublisherSize:16/unsigned, PublisherBin/binary, RandomBytes/binary>>,
    {Seq, Payload}.

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

decode_seq_header_with_publisher(<<TimeNs:64/unsigned, Seq:64/unsigned, 
                                   PublisherSize:16/unsigned, Rest/binary>>) ->
    <<PublisherBin:PublisherSize/binary, Payload/binary>> = Rest,
    PublisherID = binary_to_atom(PublisherBin, utf8),
    {Seq, TimeNs, PublisherID, Payload};

decode_seq_header_with_publisher(Bin) ->
    % Fallback for old format
    {Seq, Time, Rest} = decode_seq_header(Bin),
    {Seq, Time, unknown, Rest}.

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
        L when is_list(L)   -> list_to_binary(L);
        I when is_integer(I) -> integer_to_binary(I)
    end.

convert_to_list(Name) ->
    case Name of
        A when is_atom(A)   -> atom_to_list(A);
        B when is_binary(B) -> binary_to_list(B);
        L when is_list(L)   -> L
    end.

% Logging functions, may move these to a better logger class in the future
log_message(Message) ->
    log_message(Message, []).

log_message(Message, Args) ->
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    FormattedMessage = io_lib:format(Message, Args),
    {{_,_,_},{Hour,Min,Sec}} = erlang:localtime(),
    io:format("[~p - ~p:~p:~p] ~s~n", [NodeName, Hour, Min, Sec, FormattedMessage]).

log_state_change(Message) ->
    log_state_change(Message, []).

log_state_change(Message, Args) ->
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    FormattedMessage = io_lib:format(Message, Args),
    {{_,_,_},{Hour,Min,Sec}} = erlang:localtime(),
    io:format("[~p - ~p:~p:~p] === ~s ===~n", [NodeName, Hour, Min, Sec, FormattedMessage]).