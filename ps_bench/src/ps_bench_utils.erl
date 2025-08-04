-module(ps_bench_utils).

%% public
-export([initialize_rng_seed/0 ,initialize_rng_seed/1, generate_payload/2, evaluate_uniform_chance/1]).

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

generate_payload(PayloadSizeMean, PayloadSizeVariance) ->
    % Calculate payload size according to a normal distribution
    FloatSize = rand:normal(PayloadSizeMean, PayloadSizeVariance),
    IntSize = erlang:round(FloatSize),

    % Generate payload
    crypto:strong_rand_bytes(IntSize).

evaluate_uniform_chance(ChanceOfEvent) when 0.0 =< ChanceOfEvent, ChanceOfEvent =< 1.0 ->
    % Get a random value N, 0.0 <= N < 1.0
    RandVal = rand:uniform(),

    % The event happens if the randomly generated number is less than the chance as a percentage
    % We want strict inequality so the event doesn't fire on 0.0 <= 0.0
    % This is fine since rand:uniform cannot generate 1.0, so a chance of 1.0 will always fire
    RandVal < ChanceOfEvent.
