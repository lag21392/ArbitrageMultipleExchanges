import ccxt as ccxt_simple
id='bingxcom'
exchange = getattr(ccxt_simple, id)()
m = exchange.load_markets()

for s in m:
    print(m[s])
    print('')
