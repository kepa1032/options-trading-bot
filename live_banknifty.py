import pandas as pd
import numpy as np
import yfinance as yf
import datetime as dt
import json
import os

# ---------------- USER PARAMETERS (LIVE DYNAMIC SPREAD) ----------------
# --- File Paths ---
state_file = "portfolio_state_options.json"
trade_log_file = "live_options_tradelog.xlsx"

# --- Strategy Parameters ---
capital = 100000
lot_size = 15
brokerage_per_leg = 20 

# --- Signal Parameters ---
short_ma_period = 20 
long_ma_period = 80  

# --- VIX-Based Risk Management ---
vix_filter_threshold = 20 
allocation_calm = 1.0     
allocation_fear = 0.5     

# --- Trade Selection Parameters ---
sell_strike_moneyness = 0.98 
spread_width_pct = 0.01 
max_dte = 7

# --- Trade Management ---
profit_target_pct = 0.80 
stop_loss_pct = 1.50   
# ----------------------------------------------------------------

# --- START: Upgraded, Bulletproof State Management Functions ---
def load_state(filename):
    """Loads the portfolio state (cash, holdings) from a JSON file."""
    default_state = {'cash': 100000.0, 'holdings': None}
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        return default_state
    try:
        with open(filename, 'r') as f:
            state = json.load(f)
            holdings = state.get('holdings')
            if (holdings and isinstance(holdings, dict) and 
                all(k in holdings for k in ['entry_date', 'expiry', 'sell_strike', 'buy_strike', 'credit_received'])):
                holdings['entry_date'] = pd.to_datetime(holdings['entry_date'])
                holdings['expiry'] = pd.to_datetime(holdings['expiry'])
                state['holdings'] = holdings
            else:
                if holdings is not None:
                    print("Warning: Incomplete holdings data found. Resetting.")
                state['holdings'] = None
            if not isinstance(state.get('cash'), (int, float)):
                 state['cash'] = default_state['cash']
            return state
    except Exception as e:
        print(f"Error loading state file: {e}. Starting fresh.")
        return default_state

def save_state(filename, state):
    """Saves the portfolio state to a JSON file."""
    state_to_save = state.copy()
    holdings_data = state_to_save.get('holdings')
    if holdings_data and isinstance(holdings_data, dict):
        holdings_copy = holdings_data.copy()
        if 'entry_date' in holdings_copy and isinstance(holdings_copy.get('entry_date'), pd.Timestamp):
            holdings_copy['entry_date'] = holdings_copy['entry_date'].isoformat()
        if 'expiry' in holdings_copy and isinstance(holdings_copy.get('expiry'), pd.Timestamp):
            holdings_copy['expiry'] = holdings_copy['expiry'].isoformat()
        state_to_save['holdings'] = holdings_copy
    with open(filename, 'w') as f:
        json.dump(state_to_save, f, indent=4)
# --- END: Upgraded State Management Functions ---
        
def log_trade(filename, trade_record):
    df = pd.DataFrame([trade_record])
    header = not os.path.exists(filename)
    mode = 'a' if not header else 'w'
    with pd.ExcelWriter(filename, mode=mode, engine='openpyxl', if_sheet_exists='overlay' if not header else None) as writer:
        start_row = 0
        if not header and 'Sheet1' in writer.book.sheetnames:
            start_row = writer.book['Sheet1'].max_row
        df.to_excel(writer, index=False, header=header, sheet_name='Sheet1', startrow=start_row)

def run_live_options_trader():
    print(f"\n--- Running Live Options Trade Check for {dt.date.today()} ---")
    
    portfolio = load_state(state_file)
    cash = portfolio['cash']
    holdings = portfolio['holdings']
    
    print("Fetching latest market data from yfinance...")
    start_date = dt.datetime.now() - dt.timedelta(days=45)
    
    try:
        underlying_df = yf.download("^NSEBANK", start=start_date, end=dt.datetime.now(), interval='15m', progress=False)
        if underlying_df.empty:
            raise ValueError("Could not download BankNifty data.")
        
        underlying_df.index = underlying_df.index.tz_convert('Asia/Kolkata').tz_localize(None)
        underlying_df.rename(columns={"Close":'close', "Open": 'open', "High": 'high', "Low": 'low', "Volume": 'volume'}, inplace=True)

        vix_df = yf.download("^INDIAVIX", start=start_date, end=dt.datetime.now(), progress=False)
        if vix_df.empty:
            raise ValueError("Could not download VIX data.")
        
        daily_vix = vix_df[['Close']].resample('D').last().ffill().rename(columns={'Close': 'vix_close'})

        underlying_df.sort_index(inplace=True)
        daily_vix.sort_index(inplace=True)
        underlying_df = pd.merge_asof(underlying_df, daily_vix, left_index=True, right_index=True)
        
    except Exception as e:
        print(f"Critical Error fetching market data: {e}")
        return

    underlying_df['short_ma'] = underlying_df['close'].rolling(window=short_ma_period).mean()
    underlying_df['long_ma'] = underlying_df['close'].rolling(window=long_ma_period).mean()
    underlying_df.dropna(inplace=True)
    
    today = underlying_df.index[-1]
    
    if holdings:
        if today.date() >= holdings['expiry'].date():
             print(f"EXIT SIGNAL: Position expiring. Closing spread.")
             pnl = holdings['credit_received'] - (2 * brokerage_per_leg)
             cash += pnl
             log_trade(trade_log_file, {'ExitDate': today, 'PnL': pnl, 'Reason': 'Expiry'})
             holdings = None
    
    if not holdings:
        # --- START: New, Robust Crossover Detection Logic ---
        # Create boolean Series for the two conditions
        currently_above = underlying_df['short_ma'] > underlying_df['long_ma']
        previously_below = underlying_df['short_ma'].shift(1) <= underlying_df['long_ma'].shift(1)
        
        # A crossover is true only where both conditions are met
        crossover_signals = underlying_df[currently_above & previously_below]
        
        # Check if the most recent signal occurred at the latest timestamp
        if not crossover_signals.empty and crossover_signals.index[-1] == today:
            is_crossover = True
            latest_signal = underlying_df.loc[today]
        else:
            is_crossover = False
        # --- END: New, Robust Crossover Detection Logic ---

        if is_crossover:
            print(f"ENTRY SIGNAL: Bullish MA Crossover detected at {today}.")
            try:
                banknifty_ticker = yf.Ticker("^NSEBANK")
                available_expiries = banknifty_ticker.options
                valid_expiries = [exp for exp in available_expiries if (dt.datetime.strptime(exp, '%Y-%m-%d') - today).days >= 0 and (dt.datetime.strptime(exp, '%Y-%m-%d') - today).days <= max_dte]
                
                if valid_expiries:
                    target_expiry_str = min(valid_expiries)
                    opt_chain = banknifty_ticker.option_chain(target_expiry_str)
                    puts = opt_chain.puts
                    
                    current_price = latest_signal['close']
                    sell_strike = int(round(current_price * sell_strike_moneyness / 100) * 100)
                    buy_strike = int(round(sell_strike * (1 - spread_width_pct) / 100) * 100)
                    
                    sell_put_row = puts[puts['strike'] == sell_strike]
                    buy_put_row = puts[puts['strike'] == buy_strike]

                    if not sell_put_row.empty and not buy_put_row.empty:
                        sell_price = sell_put_row['lastPrice'].iloc[0]
                        buy_price = buy_put_row['lastPrice'].iloc[0]
                        
                        vix_level = latest_signal['vix_close']
                        position_multiplier = allocation_calm if vix_level < vix_filter_threshold else allocation_fear
                        
                        credit_received = (sell_price - buy_price) * lot_size * position_multiplier
                        
                        if credit_received > 0:
                            print(f"PAPER TRADE: Entering Bull Put Spread. Credit: {credit_received:.2f}")
                            cash += credit_received - (2 * brokerage_per_leg * position_multiplier)
                            holdings = {
                                'entry_date': today,
                                'expiry': dt.datetime.strptime(target_expiry_str, '%Y-%m-%d'),
                                'sell_strike': sell_strike,
                                'buy_strike': buy_strike,
                                'credit_received': credit_received
                            }
                            log_trade(trade_log_file, {'EntryDate': today, 'Credit': credit_received, 'SellStrike': sell_strike, 'BuyStrike': buy_strike})
            except Exception as e:
                print(f"Could not process entry signal: {e}")

    # --- Save final state and print report ---
    portfolio['cash'] = cash
    portfolio['holdings'] = holdings
    save_state(state_file, portfolio)
    
    print("\n--- Current Portfolio State ---")
    print(f"Cash: ₹{cash:,.2f}")
    if holdings:
        print("Live Position:")
        print(f"  - Strategy: Bull Put Spread")
        print(f"  - Entry Date: {holdings['entry_date'].strftime('%Y-%m-%d %H:%M')}")
        print(f"  - Expiry: {holdings['expiry'].strftime('%Y-%m-%d')}")
        print(f"  - Sell Strike: {holdings['sell_strike']:.2f}")
        print(f"  - Buy Strike: {holdings['buy_strike']:.2f}")
    else:
        print("Live Position: None (Currently in cash)")
    print("---------------------------------")


if __name__ == '__main__':
    run_live_options_trader()

