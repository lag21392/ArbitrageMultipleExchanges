import defi.defi_tools as dft
import requests
def getPairs():
    exchanges = ['PancakeSwap (v2)', 'Curve (Avalanche)', 'uniswap', 'uniswap','Compound', 'AAVE','sushiswap','anchor']
    pairs = dft.pcsPairs(as_df=False)
    for d in list(pairs['data'])[0:10]:
        for atribute in pairs['data'][d]:
            info=pairs['data'][d][atribute]
            print(f'{atribute}: {info}')         
        try:   
            crypto=str(pairs['data'][d]['base_name'].split(' ')[0]).lower()
            df = dft.geckoMarkets(crypto).sort_values(['last','volume'],ascending=False)
            #df=df[df.exchange == 'Uniswap (v3)']
            print(df.head(20))
            df.to_csv(crypto+'.csv')
        except:
            pass
        print('---------------------------------------------------------')

'''print(dft.pcsTokenInfo('cake'))
print(dft.pcsPairInfo("cake",'usdt'))'''
getPairs()
def echanges():
    url = f"https://api.coingecko.com/api/v3/exchanges/list"
    r = requests.get(url).json()
    for e in r:
        print(e)

#echanges()