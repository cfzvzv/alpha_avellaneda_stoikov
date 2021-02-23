from backtest.score_enum import ScoreEnum


class GAConfiguration:

    elite_population_percent = 0.1
    population = 5
    crossover_prob = 0.3
    sigma = 2
    decay = 0.1
    score_column = (
        ScoreEnum.sharpe
    )  # can be ,asymmetric_dampened_pnl ,open_pnl ,close_pnl ,sharpe

    def __str__(self):
        return (
            'GAConfiguration:\n'
            '\telite_population_percent: %.2f\n'
            '\tcrossover_prob: %.2f\n'
            '\tpopulation: %.2f\n'
            '\tsigma: %.2f\n'
            '\tdecay: %.3f\n'
            '\tscore_column: %s\n'
            % (
                self.elite_population_percent,
                self.crossover_prob,
                self.population,
                self.sigma,
                self.decay,
                self.score_column,
            )
        )
