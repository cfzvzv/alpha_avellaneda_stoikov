import pandas as pd
import numpy as np
from joblib import Memory
from numba import njit

import collections


@njit
def __calculate_open_returns_numba__(trades_input_reversed, best_bid, best_ask):
    position_column = 2
    quantity_column = 0
    price_column = 1

    last_side_position = 0
    open_returns = np.zeros(trades_input_reversed.shape[0])
    total_position = trades_input_reversed[0][position_column]
    total_position_sign = np.sign(total_position)
    exit_price = 0
    if total_position > 0:
        exit_price = best_bid
    elif total_position < 0:
        exit_price = best_ask
    else:
        return open_returns
    for index in range(trades_input_reversed.shape[0]):
        if total_position == 0:
            break
        row = trades_input_reversed[index][:]
        quantity = row[quantity_column]
        position = row[position_column]
        side = np.sign(row[quantity_column])
        side_position = np.sign(row[position_column])
        entry_price = row[price_column]
        if side == total_position_sign:
            if np.abs(quantity) <= np.abs(total_position):
                open_return = quantity * (exit_price - entry_price)
                total_position = total_position - quantity
                open_returns[index] = open_return
            elif np.abs(quantity) > np.abs(total_position):
                open_return = total_position * (exit_price - entry_price)
                total_position = 0
                open_returns[index] = open_return

    return open_returns


@njit
def __calculate_close_returns_numba__(trades_input):
    '''

    :param trades_input: trades
    :return: closed returns , open bids or open asks dictionaries
    '''
    position_column = 2
    quantity_column = 0
    price_column = 1

    last_side_position = 0
    last_row = None

    buy_dict = dict()
    sell_dict = dict()

    closed_returns = np.zeros(trades_input.shape[0])

    for index in range(trades_input.shape[0]):
        row = trades_input[index][:]
        position = row[position_column]
        side = np.sign(row[quantity_column])
        side_position = np.sign(row[position_column])
        # To add directly to quantities dict
        quantity_to_save_dict = abs(row[quantity_column])
        if last_row is None:
            # first iteration
            pass
        else:
            if side == last_side_position:
                pass
            else:
                position_abs = np.abs(last_row[position_column])
                quantity_abs = np.abs(row[quantity_column])
                close_quantity = position_abs
                if close_quantity > quantity_abs:
                    close_quantity = quantity_abs
                # close_quantity = np.min([position_abs,quantity_abs ])  # close the previous position
                remaining_position = close_quantity
                quantity_to_save_dict = abs(row[quantity_column])
                closed_return = 0
                iterations = 0
                while remaining_position != 0:
                    # while remaining_position > 1e-6 or remaining_position < -1e-6:
                    # if iterations > 2000:
                    #     print(
                    #         'Error calculating pnl iterations infinite to calculate closed returns '
                    #     )
                    #     break
                    if side < 0 and len(buy_dict) != 0:
                        # search the entry point in buys - loiwest price first
                        # entry_price = sorted(list(buy_dict.keys()), reverse=False)[0]
                        # fifo
                        entry_price = list(buy_dict.keys())[0]

                        entry_quantity = buy_dict[entry_price]
                        entry_quantity = min(entry_quantity, abs(remaining_position))

                        quantity_to_save_dict -= entry_quantity
                        remaining_position -= entry_quantity
                        exit_price = row[price_column]
                        closed_return += entry_quantity * (exit_price - entry_price)

                        entry_remain_position = buy_dict[entry_price] - entry_quantity
                        if entry_remain_position == 0:
                            del buy_dict[entry_price]
                        else:
                            buy_dict[entry_price] = entry_remain_position
                    elif side < 0 and len(buy_dict) == 0:
                        break

                    elif side > 0 and len(sell_dict) != 0:
                        # search the entry point in sells , highest price first
                        # entry_price = sorted(list(sell_dict.keys()), reverse=True)[0]

                        # fifo
                        entry_price = list(sell_dict.keys())[0]

                        entry_quantity = sell_dict[entry_price]
                        entry_quantity = min(entry_quantity, abs(remaining_position))

                        quantity_to_save_dict -= entry_quantity
                        remaining_position -= entry_quantity
                        closed_return += -entry_quantity * (
                            row[price_column] - entry_price
                        )

                        entry_remain_position = sell_dict[entry_price] - entry_quantity
                        if entry_remain_position == 0:
                            del sell_dict[entry_price]
                        else:
                            sell_dict[entry_price] = entry_remain_position

                    elif side > 0 and len(sell_dict) == 0:
                        break
                    iterations += 1

                closed_returns[index] = closed_return

        if quantity_to_save_dict > 0:
            if side > 0:
                if row[price_column] not in list(buy_dict.keys()):
                    buy_dict[
                        row[price_column]
                    ] = quantity_to_save_dict  # row['quantity']
                else:
                    buy_dict[
                        row[price_column]
                    ] = +quantity_to_save_dict  # row['quantity']
                # buy_dict = (sorted(buy_dict.items()))
            else:
                if row[price_column] not in list(sell_dict.keys()):
                    sell_dict[
                        row[price_column]
                    ] = quantity_to_save_dict  # row['quantity']
                else:
                    sell_dict[
                        row[price_column]
                    ] = +quantity_to_save_dict  # row['quantity']
                # sell_dict = (reversed(sorted(sell_dict.items())))
        last_side_position = side_position
        last_side = side
        last_row = row
        last_index = row

    return closed_returns, buy_dict, sell_dict


def __calculate_closed_returns_trades_numba__(trades_input: pd.DataFrame) -> tuple:
    trades_output = trades_input.copy()
    # close returns
    numba_closed_returns = 0
    bid_dict = {}
    ask_dict = {}
    if trades_input is not None and len(trades_input) > 0:
        matrix_input = trades_input.values[:, 2:].astype('float64')
        numba_closed_returns, bid_dict, ask_dict = __calculate_close_returns_numba__(
            trades_input=matrix_input
        )
    trades_output['close_returns'] = numba_closed_returns
    return trades_output, bid_dict, ask_dict


def __calculate_closed_returns_trades__(trades_input: pd.DataFrame) -> tuple:
    '''

    :param trades_input: dataframe with trades hapen
    :return: closed returns , dictionry pendig bids , dictionary pending asks
    '''
    # change to numba
    # return self.calculate_returns_trades_pandas(trades_input=trades_input)
    return __calculate_closed_returns_trades_numba__(trades_input=trades_input)





def get_asymetric_dampened_reward(trade_df: pd.DataFrame):
    return trade_df['asymmetric_dampened_pnl'].iloc[-1]







def get_max_drawdowns(backtest_pnl):
    equity_curve = backtest_pnl.cumsum()
    i = np.argmax(
        np.maximum.accumulate(equity_curve.values) - equity_curve.values
    )  # end of the period
    if i == 0:
        MDD_start, MDD_end, MDD_duration, drawdown, UW_dt, UW_duration = (
            0,
            0,
            0,
            0,
            0,
            0,
        )
        return MDD_start, MDD_end, MDD_duration, drawdown, UW_dt, UW_duration
    j = np.argmax(equity_curve.values[:i])  # start of period

    drawdown = 100 * ((equity_curve[i] - equity_curve[j]) / equity_curve[j])

    DT = equity_curve.index.values

    start_dt = pd.to_datetime(str(DT[j]))
    MDD_start = start_dt.strftime("%Y-%m-%d")

    end_dt = pd.to_datetime(str(DT[i]))
    MDD_end = end_dt.strftime("%Y-%m-%d")

    NOW = pd.to_datetime(str(DT[-1]))
    NOW = NOW.strftime("%Y-%m-%d")

    MDD_duration = np.busday_count(MDD_start, MDD_end)
    time_difference = end_dt - start_dt
    try:
        UW_dt = (
            equity_curve[i:]
            .loc[equity_curve[i:].values >= equity_curve[j]]
            .index.values[0]
        )
        UW_dt = pd.to_datetime(str(UW_dt))
        UW_dt = UW_dt.strftime("%Y-%m-%d")
        UW_duration = np.busday_count(MDD_end, UW_dt)
    except:
        UW_dt = "0000-00-00"
        UW_duration = np.busday_count(MDD_end, NOW)

    return MDD_start, MDD_end, time_difference, drawdown, UW_dt, UW_duration


def get_drawdown(backtest_pnl):
    data_ser_df = backtest_pnl  # backtest_returns.cumsum()
    # return data_ser_df.expanding(1).max() - data_ser_df
    highest_value = data_ser_df.cummax()
    return highest_value - data_ser_df
