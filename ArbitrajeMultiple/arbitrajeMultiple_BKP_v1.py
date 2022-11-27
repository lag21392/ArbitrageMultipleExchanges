import ccxt.async_support as ccxt
import ccxt as ccxt_simple
from asyncio import *
import os
import sys
from pprint import pprint
import functools
from multiprocessing import Process, Manager,cpu_count
import time
#from numpy import short
import pandas as pd
import msvcrt as m
import networkx as nx
from datetime import datetime
import matplotlib.pyplot as plt
timeSleep=10

def processTreads(symbols,ids,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,currency,exchange):
    n=int(cpu_count())
    print(n)

    def nCortes(ids,n):
        if (len(ids)//n) == (len(ids)/n):
            return len(ids)//n
        else:
            return (len(ids)//n) + 1
    listasIds=[ids[i:i + n] for i in range(0, len(ids), nCortes(ids,n))]
    print('TotalTikets: {} TiketsXProcesos: {} CantProcesos: {}'.format(len(ids)*len(symbols), len(listasIds)*len(symbols), nCortes(ids, n)))
    
    ps=[]
    nProcess=1
    p=Process(target=put_tiket, args=[nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,timeSleep])
    ps.append(p)
    nProcess=2
    p=Process(target=print_tiket, args=[nProcess,tiketsProcessDict,mutex_tiketsProcessList,timeSleep,currency,exchange,symbols])
    ps.append(p)

    for ids in listasIds:
        nProcess=nProcess+1
        p=Process(target=processAsyncs, args=[symbols,ids,nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,timeSleep])
        ps.append(p)
    for p in ps:
        p.start()
    for p in ps:
        p.join()


def processAsyncs(symbols_e1,ids,nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,timeSleep):

    global tikets
    tikets=[]
    loop = get_event_loop()
    for id in ids:
        try:
            exchange = getattr(ccxt_simple, id)()
            symbols_e2 = list(exchange.load_markets())
            symbols=list(filter(lambda x: x in symbols_e2,symbols_e1))

            loop.create_task(get_ticker(symbols, id,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,nProcess))
        except Exception as e:
            pass
    #loop.create_task(print_tiket(nProcess))
    loop.run_forever()

async def get_ticker(symbols, id,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,nProcess):


    while 1:
        for symbol in symbols:
            try:
                exchange = getattr(ccxt, id)()
                t=await exchange.fetch_ticker(symbol)
                await exchange.close()
                if t['close'] != None and t['close'] > 0 and t['baseVolume'] != None and t['baseVolume'] > 0:
                    t['id']=id
                    t['nProcess']=nProcess
                    t['date']=datetime.now()
                    mutex_tiketsProcessList.acquire()
                    tiketsProcessList.append(t)
                    mutex_tiketsProcessList.release()
                else:
                    symbols.remove(symbol)
            except Exception as e:
                #print(f'{e} {symbol}')
                #symbols.remove(symbol)
                #removiendo de diccionario general
                mutex_tiketsProcessDict.acquire()
                key=symbol + '_' + id
                if key in tiketsProcessDict:
                    tiketsProcessDict.pop(key)
                    #print(f"Removido {key}")
                mutex_tiketsProcessDict.release()
                await exchange.close()
                if len(symbols)==0:
                    break

        if len(symbols)==0:
            break
        await sleep(timeSleep)
def put_tiket(nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,timeSleep):
    while 1:

        mutex_tiketsProcessList.acquire()
        tikets=tiketsProcessList
        mutex_tiketsProcessList.release()
        if len(tikets)>0:
            t=tikets.pop(0)
            id=t['symbol'] + '_' + t['id']
            mutex_tiketsProcessDict.acquire()
            tiketsProcessDict[id] = {'symbol': t['symbol'], 'close': t['close'], 'bid': t['bid'], 'ask': t['ask'],
                                     'baseVolume': t['baseVolume'], 'nProcess': t['nProcess']}
            mutex_tiketsProcessDict.release()
            '''mutex_tiketsProcessDict.acquire()
            if tiketsProcessDict.get(id)==None:
                #primer insert
                tiketsProcessDict[id]={'symbol':t['symbol'],'close':t['close'],'bid':t['bid'],'ask':t['ask'],'baseVolume':t['baseVolume'],'nProcess':t['nProcess']}
            elif False:
                #inserta analisando volumen medio
                countsIdsIguales=0
                acumVolumen=0
                for key,value in tiketsProcessDict.items():
                    if key.split('_')[0]==t['symbol']:
                        countsIdsIguales=countsIdsIguales+1
                        acumVolumen=acumVolumen+value['baseVolume']
                if countsIdsIguales>0 and acumVolumen>0:
                    minimoPermitidoDeVolumen=acumVolumen/countsIdsIguales
                else:
                    minimoPermitidoDeVolumen=0
                if t['baseVolume']>(minimoPermitidoDeVolumen):
                    tiketsProcessDict[id] = {'symbol': t['symbol'], 'close': t['close'],'bid': t['bid'], 'ask': t['ask'],'baseVolume': t['baseVolume'],'nProcess': t['nProcess']}
                else:
                    tiketsProcessDict.pop(id)
            else:
                #inserta sin analisar volumen medio
                tiketsProcessDict[id] = {'symbol': t['symbol'], 'close': t['close'], 'bid': t['bid'], 'ask': t['ask'],
                                         'baseVolume': t['baseVolume'], 'nProcess': t['nProcess']}
            mutex_tiketsProcessDict.release()'''

            #print('{} {} {} Process:{}'.format(t['symbol'], round(t['close'],0),t['id']))
            #print(f"{t['symbol']} {t['close']} {t['baseVolume']}")

        else:
            time.sleep(1)
def print_tiket(nProcess,tiketsProcessDict,mutex_tiketsProcessDict,timeSleep,currency,exchange,symbols):
    os.system("cls")
    print(f'Inicio:{exchange} {currency} Mercados:{symbols}')
    tiempo=0
    tiempo_n=timeSleep*6
    while 1:
        mutex_tiketsProcessDict.acquire()
        tikets = tiketsProcessDict.items()
        print(f"Print Cantidad de tikets:{len(tikets)} Grafo: ",end='')
        mutex_tiketsProcessDict.release()
        if len(tikets)>0 and (tiempo<(tiempo+tiempo_n) or m.getch()== b"\r"): #and (tiempo<(tiempo+n) or m.getch()== b"\r"):
            data=[]

            for key, value in tikets:
                #pprint(t)
                data.append([key.split('_')[1],value['symbol'],value['close'],value['bid'],value['ask'],value['nProcess']])
            df=pd.DataFrame(data,columns=['Exchange','Symbol','Close','Bid','Ask','NProcess'])
            #print(df.head())
            #print('Precio Close Mas Alto')
            #print(df.sort_values(['Symbol','Close']))

            #print('Precio Close Mas Bajo')
            #print(df.sort_values(['Symbol','Close'],ascending=False).head())

            g=cargarGrafo(df,currency)

            #nx.draw_circular(g,node_color="lightblue",edge_color="gray",font_size=24,whith=2,with_labels=True,node_size=3500)
            #plt.show()
            caminos=[]
            busqueda = []
            if exchange!=None:
                sources=[currency+'_'+exchange]
            else:
                sources=g.nodes
            for node1 in sources:
                for node2 in g.nodes:
                    if node1 != node2 and currency==node1.split('_')[0] and currency==node2.split('_')[0] and node1.split('_')[0] == node2.split('_')[0]:
                        try:
                            #busqueda=list(nx.dijkstra_path(g,source=node1,target=node2,weight='close'))
                            busqueda=list(nx.all_shortest_paths(g,source=node1,target=node2,weight='close'))

                            #caminos.append(busqueda)

                        except Exception as e:
                            busqueda=[]
                            if str(e).count('not found in graph')>0:
                                continue
                            else:
                                #print(f'ERROR {source} -> {key}')
                                pass
                        caminos = caminos + busqueda
            
            try:
                
                mostrar_camino(g,caminos)
            except Exception as e:
                print(e)
                
                        #print(e)
                        #exit()
            #nx.all_shortest_paths(g,source='BTCUSDT')
            '''for g in g.edges:
                print('{} {} {} {}'.format(g[1],g[1],g['CloseIzq-Der'],g['CloseDer-Izq']))'''
            print("----------------------------------------------------------------")
            
        time.sleep(tiempo_n)

def mostrar_camino(g,conjuntoCaminos):
    conjuntoCaminosMejorado=[]
    for path in conjuntoCaminos:
        caminos=[]
        c1=g[path[0]][path[1]]['close']
        c2=g[path[2]][path[3]]['close']
        ganancia = ((c2*100) / c1) - 100
        if c1!=0 and c2!=0 and ganancia*100>1 and len(path)==4:
            caminos.append((path[0], path[1], 1, round(g[path[0]][path[1]]['close'], 4)))
            caminos.append((path[1], path[2], 1, round(g[path[1]][path[2]]['close'], 4)))
            caminos.append((path[2], path[3], round(ganancia, 1), round(g[path[2]][path[3]]['close'], 4)))
            conjuntoCaminosMejorado.append(caminos)
    def prinCamino(caminos,g):

        n=80
        print(g)
        #print(f'CantNodos:{len(list(g.nodes))} CantCaminos:{len(list(g.edges))}')
        print(f"Caminos con porcentaje de ganancia TOP {n}")
        print('____________________________________________________________________________________________')
        for camino in caminos[0:n - 1]:
            for c in camino[:-1]:
                print('{} ({})> '.format(c[0],c[3]),end="")
            c=camino[-1]
            print('{} ({})> {} = {}% '.format(c[0], c[3], c[1], c[2]))




    conjuntoCaminosMejorado.sort(key=lambda camino: camino[-1][2],reverse=True)
    prinCamino(conjuntoCaminosMejorado,g)

def cargarGrafo(df,currency):
    grafo = nx.DiGraph()

    for i,d in df.iterrows():
        if d['Close'] >0:
            '''if d['Exchange']=='binance':
                pass'''
            node1=d['Symbol'].split('/')[0]+'_'+d['Exchange']
            grafo.add_node(node1)
            node2=d['Symbol'].split('/')[1]+'_'+d['Exchange']
            grafo.add_node(node2)
            grafo.add_edge(node1, node2, close=d['Close'], porcentaje=1)
            grafo.add_edge(node2, node1, close=d['Close'], porcentaje=1)
    for node1 in grafo.nodes:
        nodes2=list(filter(lambda x: node1.split('_')[0]==x.split('_')[0] and currency!=node1.split('_')[0] and currency!=x.split('_')[0] and node1!=x,grafo.nodes))
        for node2 in nodes2:
            grafo.add_edge(node1, node2, close=1, porcentaje=1)
            grafo.add_edge(node2, node1, close=1, porcentaje=1)
    return grafo


if __name__ == '__main__':
    with Manager() as manager:
        nMercados=20
        currency = 'BTC'
        #exchange ='binance'
        exchange = None
        exchangesANoUsar = ['crex24','bytetrade']#['crex24','wavesexchange']

        exchangeInstance = getattr(ccxt_simple, 'binance')()
        symbols = list(filter(lambda x: currency in list(x.split('/')) ,list(exchangeInstance.load_markets())))[:nMercados]
        mutex_tiketsProcessList = manager.Lock()
        tiketsProcessList=manager.list([])
        mutex_tiketsProcessDict = manager.Lock()
        tiketsProcessDict=manager.dict()

        exchanges=list(filter(lambda x: x not in exchangesANoUsar,ccxt.exchanges))

        processTreads(symbols,exchanges,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,currency,exchange)