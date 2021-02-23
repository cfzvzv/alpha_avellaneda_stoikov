class AlgorithmEnum:
    avellaneda_q = "AvellanedaQ"
    avellaneda_dqn = "AvellanedaDQN"
    avellaneda_stoikov = "AvellanedaStoikov"
    constant_spread = "ConstantSpread"
    sma_cross = "SMACross"
    rsi = "RSI"


def get_algorithm(algorithm_enum: AlgorithmEnum):
    from backtest.avellaneda_q import AvellanedaQ
    from backtest.avellaneda_stoikov import AvellanedaStoikov
    from backtest.avellaneda_dqn import AvellanedaDQN
    from backtest.sma_cross import SMACross
    from backtest.rsi import RSI

    if algorithm_enum == AlgorithmEnum.avellaneda_q:
        return AvellanedaQ
    if algorithm_enum == AlgorithmEnum.avellaneda_stoikov:
        return AvellanedaStoikov
    if algorithm_enum == AlgorithmEnum.avellaneda_dqn:
        return AvellanedaDQN
    if algorithm_enum == AlgorithmEnum.sma_cross:
        return SMACross
    if algorithm_enum == AlgorithmEnum.rsi:
        return RSI
