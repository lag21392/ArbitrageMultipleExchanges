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
import networkx as nx
from datetime import datetime
import matplotlib.pyplot as plt
import json
timeSleep=1

def processTreads(symbols,ids,currencys,exchange):
    n=int(cpu_count())-1


    def nCortes(ids,n):
        if (len(ids)//n) == (len(ids)/n):
            return len(ids)//n
        else:
            return (len(ids)//n) + 1
    tamanioTramo=nCortes(ids,n)
    listasIds=[ids[i:i + tamanioTramo] for i in range(0, len(ids), tamanioTramo)]
    print('TotalTikets: {} TiketsXProcesosMedia: {} CantProcesos: {}'.format(len(ids)*len(symbols), len(ids)*len(symbols)//n,n))
    #Variables compartidas
    mutex_tiketsProcessList = manager.Lock()
    tiketsProcessList = manager.list([])
    mutex_tiketsProcessDict = manager.Lock()
    tiketsProcessDict = manager.dict()
    mutex_controlTareasEjecutadas = manager.Lock()
    controlTareasEjecutadas = manager.dict()
    #--------------------------------
    ps=[]
    nProcess=1
    p=Process(target=put_tiket, args=[nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,controlTareasEjecutadas,mutex_controlTareasEjecutadas,timeSleep])
    ps.append(p)
    mutex_controlTareasEjecutadas.acquire()
    controlTareasEjecutadas['tarea_put_tiket'] = True
    mutex_controlTareasEjecutadas.release()
    nProcess=2
    p=Process(target=print_tiket, args=[nProcess,tiketsProcessDict,mutex_tiketsProcessList,controlTareasEjecutadas,mutex_controlTareasEjecutadas,timeSleep,currencys,exchange,symbols])
    ps.append(p)
    mutex_controlTareasEjecutadas.acquire()
    controlTareasEjecutadas['tarea_print'] = True
    mutex_controlTareasEjecutadas.release()
    for ids in listasIds:
        nProcess=nProcess+1
        mutex_controlTareasEjecutadas.acquire()
        controlTareasEjecutadas['tareaExtraccion_'+str(nProcess)]=False
        mutex_controlTareasEjecutadas.release()
        p=Process(target=processAsyncs, args=[symbols,ids,nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,controlTareasEjecutadas,mutex_controlTareasEjecutadas,timeSleep])
        ps.append(p)
    for p in ps:
        p.start()
    for p in ps:
        p.join()


def processAsyncs(symbols_e1,ids,nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,controlTareasEjecutadas,mutex_controlTareasEjecutadas,timeSleep):
    global tikets
    tikets=[]
    loop = new_event_loop()
    for id in ids:
        try:
            exchange = getattr(ccxt_simple, id)()
            symbols_e2 = list(exchange.load_markets())
            symbols=list(filter(lambda x: x in symbols_e2,symbols_e1))

            loop.create_task(get_ticker(symbols, id,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,controlTareasEjecutadas,mutex_controlTareasEjecutadas,nProcess))
        except Exception as e:
            pass
    #loop.create_task(print_tiket(nProcess))
    loop.run_forever()

async def get_ticker(symbols, id,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,controlTareasEjecutadas,mutex_controlTareasEjecutadas,nProcess):
    while 1:
        with open('config.json', 'r') as f:
            volumen = json.load(f)["volumen"]
        mutex_controlTareasEjecutadas.acquire()
        try:
            esperar=controlTareasEjecutadas.get('tareaExtraccion_'+str(nProcess))
        except:
            esperar=True
        mutex_controlTareasEjecutadas.release()
        if not esperar :
            for symbol in symbols:
                cantidadIntentos=2
                while cantidadIntentos>0:
                    try:
                        exchange = getattr(ccxt, id)()
                        t=await exchange.fetch_ticker(symbol)
                        await exchange.close()
                        if t['close'] != None and t['close'] > 0 and t['baseVolume'] != None and t['baseVolume'] > 0 and t['baseVolume']*t['close']>volumen:
                            t['id']=id
                            t['nProcess']=nProcess
                            t['date']=datetime.now()
                            mutex_tiketsProcessList.acquire()
                            tiketsProcessList.append(t)
                            mutex_tiketsProcessList.release()
                        else:
                            symbols.remove(symbol)
                        cantidadIntentos=0
                    except Exception as e:
                        
                        if str(e).find('fetchTicker() is not supported yet')>0 or str(e).find("is not iterable")>0 or str(e).find("error")>0:
                            cantidadIntentos=0
                            if symbol in symbols:
                                symbols.remove(symbol)
                        else:
                            print(e)
                            cantidadIntentos=cantidadIntentos-1
                            
                        await exchange.close()                        
                        
                        #print(f'{e} {symbol}')
                        #symbols.remove(symbol)
                        #removiendo de diccionario general
                        if cantidadIntentos==0:
                            #if symbol in symbols:
                            #    symbols.remove(symbol)
                            mutex_tiketsProcessDict.acquire()
                            key=symbol + '_' + id
                            if key in tiketsProcessDict:
                                tiketsProcessDict.pop(key)
                                #print(f"Removido {key}")
                            mutex_tiketsProcessDict.release()
                        else:
                            await sleep(1)
                        
                        
                        
                if len(symbols)==0:
                    break
            mutex_controlTareasEjecutadas.acquire()
            controlTareasEjecutadas['tareaExtraccion_'+str(nProcess)]=True            
            mutex_controlTareasEjecutadas.release()
            if len(symbols)==0:
                break

        await sleep(1)
def put_tiket(nProcess,tiketsProcessList,tiketsProcessDict,mutex_tiketsProcessList,mutex_tiketsProcessDict,controlTareasEjecutadas,mutex_controlTareasEjecutadas,timeSleep):
    while 1:
        mutex_controlTareasEjecutadas.acquire()
        listaTareasEjecutadas=controlTareasEjecutadas
        mutex_controlTareasEjecutadas.release()
        tareasEjecutadas=True
        for key,value in listaTareasEjecutadas.items():
            if str(key).count('tareaExtraccion')>0:
                tareasEjecutadas=tareasEjecutadas and value
        mutex_controlTareasEjecutadas.acquire()
        controlTareasEjecutadas['tarea_put_tiket']=False
        esperar=controlTareasEjecutadas['tarea_put_tiket']
        mutex_controlTareasEjecutadas.release()
        
            
        if tareasEjecutadas and not esperar:

            mutex_tiketsProcessList.acquire()
            tikets=tiketsProcessList
            mutex_tiketsProcessList.release()
            
            while len(tikets)>0:
                t=tikets.pop(0)
                id=t['symbol'] + '_' + t['id']
                mutex_tiketsProcessDict.acquire()
                tiketsProcessDict[id] = {'symbol': t['symbol'], 'close': t['close'], 'bid': t['bid'], 'ask': t['ask'],
                                         'baseVolume': t['baseVolume'], 'nProcess': t['nProcess']}
                mutex_tiketsProcessDict.release()
            mutex_controlTareasEjecutadas.acquire()            
            controlTareasEjecutadas['tarea_put_tiket'] = True
            for key, value in controlTareasEjecutadas.items():
                if str(key).count('tareaExtraccion') > 0:
                    controlTareasEjecutadas[key]=False
            controlTareasEjecutadas['tarea_print'] = False
            mutex_controlTareasEjecutadas.release()
        time.sleep(1)
def print_tiket(nProcess,tiketsProcessDict,mutex_tiketsProcessDict,controlTareasEjecutadas,mutex_controlTareasEjecutadas,timeSleep,currencys,exchange,symbols):

    os.system("clear")
    print(f'Definicion de Inicio -> Exchange:{exchange} Cripto: {currencys} Mercados:\n{symbols}')

    while 1:

        mutex_controlTareasEjecutadas.acquire()
        esperar=controlTareasEjecutadas.get('tarea_print')
        mutex_controlTareasEjecutadas.release()

        if not esperar:
            mutex_tiketsProcessDict.acquire()
            tikets = tiketsProcessDict.items()
            print(f"Print Cantidad de tikets:{len(tikets)} Grafo: ",end='')
            mutex_tiketsProcessDict.release()
            if len(tikets)>0:
                data=[]

                for key, value in tikets:
                    #pprint(t)
                    data.append([key.split('_')[1],value['symbol'],value['close'],value['bid'],value['ask'],value['baseVolume'],value['nProcess']])
                df=pd.DataFrame(data,columns=['Exchange','Symbol','Close','Bid','Ask','BaseVolume','NProcess'])
                #print(df.head())
                #print('Precio Close Mas Alto')
                #print(df.sort_values(['Symbol','Close']))
                #print('Precio Close Mas Bajo')
                #print(df.sort_values(['Symbol','Close'],ascending=False).head())
                #nx.draw_circular(g,node_color="lightblue",edge_color="gray",font_size=24,whith=2,with_labels=True,node_size=3500)
                #plt.show()
                g = cargarGrafo(df, currencys)
                caminos=[]
                for currency in currencys:
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
                                    busqueda=list(filter(lambda x:len(x)==4,busqueda))
                                    #caminos.append(busqueda)
                                    caminos = caminos + busqueda
                                except Exception as e:
                                    busqueda=[]
                                    if str(e).count('not found in graph')>0:
                                        continue
                                    else:
                                        #print(f'ERROR {source} -> {key}')
                                        pass


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
                mutex_controlTareasEjecutadas.acquire()

                controlTareasEjecutadas['tarea_print'] = True
                mutex_controlTareasEjecutadas.release()
        
        time.sleep(timeSleep)
        #time.sleep(2*60)

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
        os.system("clear")
        
        n=2000
        print(g)
        print(f"Caminos con porcentaje de ganancia TOP {n}")
        print('____________________________________________________________________________________________')

        date_time=datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        columns=['time','buy_market','buy_exchange','buy_price','sell_market','sell_exchange','sell_price','%_profit']
        df = pd.DataFrame(columns=columns)
        for camino in caminos[0:n - 1]:
            if camino[0][3]!=0:
                d={'time':date_time,
                'buy_market':[camino[0][0].split('_')[0]+'/'+camino[0][1].split('_')[0]],'buy_exchange':[camino[0][0].split('_')[1]],'buy_price':[camino[0][3]],
                'sell_market':[camino[2][1].split('_')[0]+'/'+camino[1][1].split('_')[0]],'sell_exchange':[camino[1][1].split('_')[1]],'sell_price':[camino[2][3]],
                '%_profit':[camino[2][2]]
                }
                df_new = pd.DataFrame(d)
                df=pd.concat([df,df_new],axis=0).reset_index(drop=True)
        print(df)
        df.to_csv('output.csv',index=False)
        df.to_json('output.json',orient="split")
    conjuntoCaminosMejorado.sort(key=lambda camino: camino[-1][2],reverse=True)
    prinCamino(conjuntoCaminosMejorado,g)

def cargarGrafo(df,currencys):
    grafo = nx.DiGraph()

    for i,d in df.iterrows():
        if d['Close'] >0:
            '''if d['Exchange']=='binance':
                pass'''
            node1=d['Symbol'].split('/')[0]+'_'+d['Exchange']
            grafo.add_node(node1)
            node2=d['Symbol'].split('/')[1]+'_'+d['Exchange']
            grafo.add_node(node2)
            grafo.add_edge(node1, node2, close=d['Close'], closeInverso=float(1/d['BaseVolume']))
            grafo.add_edge(node2, node1, close=d['Close'], closeInverso=float(1/d['BaseVolume']))
    for currency in currencys:
        for node1 in grafo.nodes:
            nodes2=list(filter(lambda x: node1.split('_')[0]==x.split('_')[0] and currency!=node1.split('_')[0] and currency!=x.split('_')[0] and node1!=x,grafo.nodes))
            for node2 in nodes2:
                grafo.add_edge(node1, node2, close=1, porcentaje=1)
                grafo.add_edge(node2, node1, close=1, porcentaje=1)
    return grafo


if __name__ == '__main__':
    with Manager() as manager:
        nMercados=100000
        currencys = ['USDT']#,'DAI','BNB','BTC','ETH']
        #exchange ='binance'
        exchange = None
        exchangesANoUsar = ['crex24','bytetrade','lykke','zb']#['crex24','wavesexchange']

        exchangeInstance = getattr(ccxt_simple, 'binance')()
        symbols=[]
        for currency in currencys:
            symbols = symbols + list(filter(lambda x: currency in list(x.split('/')) ,list(exchangeInstance.load_markets())))[:nMercados]

        exchanges=list(filter(lambda x: x not in exchangesANoUsar,ccxt.exchanges))

        processTreads(symbols,exchanges,currencys,exchange)