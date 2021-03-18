#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import requests
#import pickle
import json
import numpy as np
import time
import math
from datetime import datetime, timedelta
#from mysql import connector
import mysql.connector
from colorama import Style, Back, Fore
from multiprocessing import Pool
from itertools import count

# CONSTANTS
# version number
_AUTHORIZATION_ = 'BFC80299-03AA-4CB2-B417-EB119DE16143|65abe6d93e264abd9f08573f4a975f64'
_V_ = '0.4'
LINESKEEP=1000
BESTPRINT=1
normalmu = 3
normalsigma = 1
_HOST_ = "127.0.0.1"
_DATABASE_ = "rakuten"
_DBUSER_ = "rakuten"
_DBPASSWORD_ = "bTxB2cJ4PhM8fUw5"
_DBSOCKET_ = "/opt/local/var/run/mariadb-10.2/mysqld.sock"

np.set_printoptions(precision=2, linewidth=99999, suppress=True, threshold=np.inf)

def get_filename_datetime(ext):
    return str(datetime.now()).replace(" ", "-").replace(":","").replace(".","")+"."+ext

class wishlist_optimiser:

    def __init__(self, args):
        print(Style.RESET_ALL, end="")
        #self.userId = self.getUserId(args)
        self.session = requests.session()
        self.setheaders()
        self.nocompute = args.no_compute
        self.update = args.whishlist
        self.prices_update = not args.no_prices_update
        self.quiet = args.quiet
        self.filein = args.input
        self.fileout = args.output
        if args.output is None:
            self.fileout = get_filename_datetime("txt")
        self.fileout = self.fileout
        self.productsId = []
        self.initDB(args)
        self.initLists()
        # self.pool = Pool(2)

    # def __getstate__(self):
    #     self_dict = self.__dict__.copy()
    #     del self_dict['pool']
    #     return self_dict

    #   def __setstate__(self, state):
    #     self.__dict__.update(state)

    def initLists(self):
        self.productsHeadLine = []
        self.productsAutor = []
        self.sellers = []
        self.prices = []
        self.shippingAmount = []
        self.nextSA = []

    def initDB(self, args):
        self.printDoing("DB initialization")
        self.mydb = mysql.connector.connect(
            host=_HOST_,
            user=_DBUSER_,
            passwd=_DBPASSWORD_,
            database=_DATABASE_,
            unix_socket=_DBSOCKET_
            )
        self.mycursor = self.mydb.cursor()
        req = "CREATE TABLE IF NOT EXISTS Seller ( ID_SELLER BIGINT UNSIGNED NOT NULL , Name TEXT NOT NULL, PRIMARY KEY( ID_SELLER) ) ENGINE = InnoDB;"
        self.mycursor.execute(req)
        self.mydb.commit()
        req = "CREATE TABLE IF NOT EXISTS Product ( ID_PRODUCT BIGINT UNSIGNED NOT NULL , Name TEXT NOT NULL, Author TEXT NOT NULL, EDITO LONGTEXT, PRIMARY KEY( ID_PRODUCT ) ) ENGINE = InnoDB;"
        self.mycursor.execute(req)
        self.mydb.commit()
        req = "CREATE TABLE IF NOT EXISTS Prices ( ID_PRODUCT BIGINT UNSIGNED NOT NULL, ID_SELLER BIGINT UNSIGNED NOT NULL, Price INT UNSIGNED, ShippingAmount INT UNSIGNED, NextSA INT UNSIGNED , PRIMARY KEY( ID_PRODUCT, ID_SELLER) ) ENGINE = InnoDB;"
        self.mycursor.execute(req)
        self.mydb.commit()
        self.printOK()
        # if self.prices_update:
        #     self.printDoing("DB truncate Prices Table")
        #     req = "TRUNCATE TABLE Prices;"
        #     self.mycursor.execute(req)
        #     self.mydb.commit()
        #     self.printOK()
        #     self.printDoing("DB truncate Product Table")
        #     req = "DELETE FROM Product;"
        #     self.mycursor.execute(req)
        #     self.mydb.commit()
        #     self.printOK()


    # def getUserId(self, args):
    #     if args.userId:
    #         try:
    #             userHashedID = int(args.userId)
    #         except ValueError:
    #             print("L'identifiant doit être un nombre.")
    #             sys.exit(1)
    #     else:
    #         cj = browsercookie.load()
    #         try:
    #             print("Loading cookies ...")
    #             userHashedID = cj._cookies["fr.shopping.rakuten.com"]["/"]["userHashedID"]
    #             print(userHashedID)
    #             userHashedID = int(userHashedID)
    #         except:
    #             print("Vous devez vous connecter à votre compte sur un navigateur (Firefox ou Chrome).")
    #             sys.exit(1)
    #     return userHashedID

    def setheaders(self, auth=False):
        headers = {
            'Accept': 'application/json',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 BuyerApp/6.1.2 (iOS; BuildNumber 139)',
            'Accept-Language': 'fr-fr',
            'Accept-Encoding': 'br, gzip, deflate',
        }
        if auth:
            headers['Authorization'] = _AUTHORIZATION_
        self.session.headers.update(headers)
        return headers

    def printOK(self, end='\n'):
        if not self.quiet:
            strspace = " "*20
            print(Style.BRIGHT+Fore.LIGHTGREEN_EX+" OK"+strspace, end="\033[K")
            print(Style.RESET_ALL, end=end)

    def printDoing(self, str, end=""):
        if not self.quiet:
            strspace = " "*5
            strdot = "."*(20-len(str))
            print(strspace+str+" "+strdot, end=end, flush=True)

    def requests_wishlist(self):
        url = 'https://ws.fr.shopping.rakuten.com/rest/memo/v1/get?channel=buyerapp&channelType=BUYER_SMARTPHONE_APP'
        url = url.rstrip()
        r = self.session.get(url, allow_redirects=False)
        j = json.loads(r.text)
        if "result" not in j:
            print("************************************** ERREUR ",
                j["code"],
                "   ******************************************" )
            print(j)
            exit(1)
        wish_dict = j["result"]
        productsID = [ p["productId"] for p in wish_dict["memos"]]
        print(productsID)
        while "paginationToken" in wish_dict:
            next_url = url+"&paginationToken="+wish_dict["paginationToken"]
            time.sleep(np.abs(np.random.normal(normalmu, normalsigma))) # essayons d'être discret !
            r = self.repeatRequestGet(next_url, 3)
            #r = self.session.get(next_url, allow_redirects=False)
            wish_dict = json.loads(r.text)["result"]
            productsID += [ p["productId"] for p in wish_dict["memos"]]
            print(productsID)
            #print(wish_dict)
        return productsID

    def get_productsId(self):
        self.printDoing("Get productsId")
        filename = self.filein
        # filename = str(self.userId)+".pkl"
        # print("filename=", filename)
        with open(filename, "r") as infile:
            productsID = json.load(infile)
            #productsID = pickle.load(infile)
        self.printOK()
        return np.array(productsID)
        
    def whishlist_download(self):
        self.printDoing("Get whishlist")
        self.setheaders(auth=True)
        with open("whishlist_"+get_filename_datetime("json"), "w") as outfile:
            productsID = self.requests_wishlist()
            json.dump(productsID, outfile)
            #pickle.dump(productsID, fileOut)
        self.setheaders(auth=False)
        self.printOK()
        return np.array(productsID)

    def get_urlForProduct(self, pid, paging=0):
        opt = [ 
                ("loadAdverts", "true"),
                ("advertType","ALL"), 
                ("channel","buyerapp"),
                ("channelType", "BUYER_SMARTPHONE_APP"), 
                #("loadBuybox", "true"),
                ("loadProductDetails","true"),
                ("loadRakuponDetails", "true"),
                ("version","6")
                ]
        if paging > 0:
            opt = [("advertPaging", str(paging))]+opt
        #https://ws.fr.shopping.rakuten.com/restpublic/vis-apps/products/991602
        #?advertPaging=15&advertType=ALL&buyerId=15069732&channel=buyerapp&channelType=BUYER_SMARTPHONE_APP&loadProductDetails=0&loadRakuponDetails=true&version=6
        url = 'https://ws.fr.shopping.rakuten.com/restpublic/vis-apps/products/'
        url = url + str(pid)
        url = url + "?"
        for e in opt :
            url = url + e[0]+"="+e[1]+"&"
        #loadRakuponDetails=false&loadAdverts=true&channelType=BUYER_SMARTPHONE_APP&version=6&advertType=ALL&loadBuybox=true&channel=buyerapp&loadProductDetails=true"
        url = url[:-1]
        url = url.rstrip()
        return url

    def msqlInsert(self, d, pid, title, author, nbsellers):
        req = "DELETE FROM Prices WHERE Id_Product="+str(pid)
        self.mycursor.execute(req)
        self.mydb.commit()
        if nbsellers<1:
            return
        req = "INSERT IGNORE INTO Seller (Id_Seller, NAME) VALUES (%s, %s) "
        val = []
        for seller in d["adverts"]:
            if "login" in seller["seller"]:
                val.append( (seller["seller"]["id"], seller["seller"]["login"]) )
            else:
                login = seller["seller"]["type"]+str(seller["seller"]["id"])
                val.append( (seller["seller"]["id"],  login  ) )
        self.mycursor.executemany(req, val)
        self.mydb.commit()
        req = "INSERT IGNORE INTO Product (ID_PRODUCT, NAME, AUTHOR, EDITO) VALUES (%s, %s, %s, %s) "
        des = d["description"] if "description" in d else ""
        val = [(int(pid), title, author, des)]
        self.mycursor.executemany(req, val)
        self.mydb.commit()
        req = "INSERT IGNORE INTO Prices (Id_Product, Id_Seller, Price, ShippingAmount, NextSA) VALUES (%s, %s, %s, %s, 100)"
        # TODO : find next shippingAmout for every article/seller
        val = [(int(pid),
                int(seller["seller"]["id"]),
                round(float(seller["salePrice"])*100),
                round(float(seller["shippingAmount"])*100)) for seller in d["adverts"]]
        self.mycursor.executemany(req, val)
        self.mydb.commit()

    def msqlSelectPrices(self):
        self.printDoing("DB requests")
        self.sellers = []
        self.prices = []
        self.shippingAmount = []
        self.nextSA = []
        for pid in self.productsId:
            req = "SELECT Name, Author FROM Product WHERE ID_PRODUCT="+str(pid)
            self.mycursor.execute(req)
            res = self.mycursor.fetchone() or ["", ""]
            self.productsHeadLine.append(res[0])
            self.productsAutor.append(res[1])
            req = "SELECT Id_Seller, Price, shippingAmount, NextSA FROM Prices WHERE ID_PRODUCT="+str(pid)
            self.mycursor.execute(req)
            res = self.mycursor.fetchall()
            if len(res)==0:
                S = np.array([-1]).reshape(-1, 1)
                P = np.array([0]).reshape(-1, 1)
                SA = np.array([0]).reshape(-1, 1)
                NSA = np.array([0]).reshape(-1, 1)
            else:
                S = np.array([int(e[0]) for e in res]).reshape(-1, 1)
                P = np.array([int(e[1]) for e in res]).reshape(-1, 1)
                SA = np.array([int(e[2]) for e in res]).reshape(-1, 1)
                NSA = np.array([int(e[3]) for e in res]).reshape(-1, 1)
            self.sellers.append(S)
            self.prices.append(P)
            self.shippingAmount.append(SA)
            self.nextSA.append(NSA)
        self.printOK()

    def appendPricesInfos(self, adverts):
        nb = len(adverts)
        if nb == 0:
            self.sellers.append(np.array([""]).reshape(-1, 1) )
            self.prices.append(np.array([0]).reshape(-1, 1))
            self.shippingAmount.append(np.array([0]).reshape(-1, 1))
            return nb
        #print(adverts)
        S = []
        for seller in adverts:
            if "login" in seller["seller"]:
                S.append(seller["seller"]["login"])
            else:
                login = seller["seller"]["type"]+str(seller["seller"]["id"])
                S.append(login)
        P = [round(float(seller["salePrice"])*100) for seller in adverts]
        SA = [round(float(seller["shippingAmount"])*100) for seller in adverts]
        _, index = np.unique(S, return_index=True)
        index.sort()
        self.sellers.append(
            np.array(S)[index].reshape(-1, 1) )
        self.prices.append(
            np.array(P)[index].reshape(-1, 1) )
        self.shippingAmount.append(
            np.array(SA)[index].reshape(-1, 1) )
        return len(index)

    def appendProdInfos(self, d):
        if "headline" in d :
            title = d["headline"]
        else :
            title = ""
        if "contributor" in d and "caption" in d['contributor']:
                author = d['contributor']["caption"]
        else :
            author = ""
        # try:
        #     title = d["headline"]            
        # except:
        #     title = ""
        # try:
        #     author = d['contributor']["caption"]
        # except:
        #     author = ""
        self.productsHeadLine.append(title)
        self.productsAutor.append(author)
        return title, author


    def wait_random_time(self, pid):
#         last = self.productsId[-1]
#         if last == pid:
#             return
        sec = np.random.normal(normalmu, normalsigma)
        while sec<0:
            sec = np.random.normal(normalmu, normalsigma)
        self.printDoing("Waiting %.2f s  "% sec, end='\r')
        time.sleep(sec)

    def repeatRequestGet(self, url, nb):
        if nb == 0:
            print("************************************** ERREUR ",
                r.status_code,
                "   ******************************************" )
            print(sys.exc_info()[0])
            print(r.text)
            print("ERROR request : number of attempts exceeded !")
            exit(1)
        try:
            r = self.session.get(url, allow_redirects=False)
        except:
            print("")
            self.wait_random_time(0)
            r = self.repeatRequestGet(url, nb-1)
        return r

    def downloadPrices(self):
        self.printDoing("Download", end="\n")
        nbproducts = len(self.productsId)
        self.sellers = []
        self.prices = []
        self.shippingAmount = []
        for i, pid in enumerate(self.productsId):
            self.printDoing("  Product  %12d (%d / %d)"% (pid, i+1, nbproducts), end="" )
            d = json.loads(self.repeatRequestGet(self.get_urlForProduct(pid), 3).text)
            try:
                totalsellers = d["advertsCount"]
            except Exception as e:
                print("Error = " )
                print(d)
                raise e

            currentNbSellers = len(d["adverts"])
            while currentNbSellers < totalsellers:
                url = self.get_urlForProduct(pid, paging=currentNbSellers)
                r = self.repeatRequestGet(url, 3)
                nextd = json.loads(r.text)
                d["adverts"] = d["adverts"] + nextd["adverts"]
                currentNbSellers = len(d["adverts"])
            nbsellers = self.appendPricesInfos(d["adverts"])

            title, author = self.appendProdInfos(d)
            self.msqlInsert(d, pid, title, author, nbsellers)
            self.printDoing("   --->   %3d/%3d %s"% (currentNbSellers, totalsellers, (" seller" if currentNbSellers<2 else "sellers")), end="")
            self.printOK()
            self.wait_random_time(pid)

    def ReduceShipAmou_SameSeller(self, Prev_Sel, Cur_Sel, Prev_SA, Cur_SA, Prev_NPSA):
        # L = zip(Prev_Sel, Cur_Sel, Prev_SA, Cur_SA)
        # self.pool.map(self.ReduceShipAmou_Parallel, L)
        for i, s in enumerate(Cur_Sel):
            PSA = Prev_SA[i]
            PS = Prev_Sel[i]
            NPSA = Prev_NPSA[i]
            mask = (s==PS)
            maxPSA = np.where(mask, PSA, -1).max()
            if maxPSA > 100:
                PSA = np.where(mask, NPSA, PSA)
                #PSA[mask] = 100 #TODO : adapter en fonction des articles
                Cur_SA[i] = np.max([maxPSA, Cur_SA[i]])

    def msqlShippingFree(self):
        req = "UPDATE Prices as P1 INNER JOIN (SELECT Id_Seller, Min(ShippingAmount) as min FROM Prices WHERE ShippingAmount<5 GROUP BY Id_Seller) as P2 ON P1.Id_Seller = P2.Id_Seller SET P1.ShippingAmount = P2.min"
        self.mycursor.execute(req)
        self.mydb.commit()

    def cartesianProduct(self, A, B):
        C = np.repeat(A, len(B), axis=0)
        D = np.tile(B, (len(A), 1) )
        return np.concatenate((C,D),axis=1), C, D

    def computeBest(self):
        Prev_Sellers = []
        Prev_Prices = []
        Prev_ShAm = []
        Ltime = [time.time()]
        nb = len(self.sellers)
        nbext = math.ceil(0.1*nb)
        for i, cur_Sellers, cur_Prices, cur_ShAm, cur_nextSA in\
                zip(count(), self.sellers, self.prices, self.shippingAmount, self.nextSA):
            #T1 = time.process_time()
            self.printDoing("\r     Computing ...........%3d %%" % round((i+1)/nb*100, 2))
            if len(Prev_Sellers) == 0:
                cost = np.sum(cur_Prices,1)+np.sum(cur_ShAm, 1)
                order = np.argsort(cost)[:LINESKEEP]
                Prev_Sellers, Prev_Prices, Prev_ShAm, Prev_NPSA = cur_Sellers[order], cur_Prices[order], cur_ShAm[order], cur_nextSA[order]
                continue
            # Make All Combinaison
            S, expand_Prev_S, expand_Cur_S = self.cartesianProduct(Prev_Sellers, cur_Sellers)
            P, _, _ = self.cartesianProduct(Prev_Prices, cur_Prices)
            _, expand_Prev_SA, expand_Cur_SA = self.cartesianProduct(Prev_ShAm, cur_ShAm)
            NSA, expand_Prev_NSA, _ = self.cartesianProduct(Prev_NPSA, cur_nextSA)
            # Reduced Shipping Amount for same seller
            #T2 = time.process_time()
            self.ReduceShipAmou_SameSeller(expand_Prev_S, expand_Cur_S, expand_Prev_SA, expand_Cur_SA, expand_Prev_NSA)
            #T3 = time.process_time()
            # compute total cost for all lines
            #print("expand_Prev_SA=",expand_Prev_SA)
            #print("expand_Cur_SA=",expand_Cur_SA)
            SA = np.concatenate( (expand_Prev_SA, expand_Cur_SA), axis=1)
            #print("SA = ", SA)
            #print("Sum_SA = ",np.sum(SA, 1))
            #T4 = time.process_time()
            cost = np.sum(P, 1)+np.sum(SA, 1)
            #T5 = time.process_time()
            # Sorting
            order = np.argsort(cost)[:LINESKEEP]
            Prev_Sellers = S[order]
            Prev_Prices = P[order]
            Prev_ShAm = SA[order]
            Prev_NPSA = NSA[order]
            #T6 = time.process_time()
            #print("Times= ", round(T2-T1,4), " / ", round(T3-T2,4), " / ", round(T4-T3,4), " / ", round(T5-T4,4), " / ", round(T6-T5,4), end="\n")
            Ltime.append(time.time())
            #tm = (Ltime[-1] - Ltime[0])/i
            t = (Ltime[-1] - Ltime[-min(nbext,len(Ltime))])/nbext
            self.printDoing("  Remaining Time :  %s " % timedelta(seconds=math.ceil((2*t)*(nb-i) )), end="\r")
        self.printDoing("Computing")
        self.printOK()
        return Prev_Sellers, Prev_Prices, Prev_ShAm, Ltime, cost[order]

    def dispResults(self, S, P, SA):
        self.printDoing("Naming")
        Sname = S.astype(str)
        req = "SELECT Name FROM Seller WHERE ID_SELLER=%s"
        for idx, x in np.ndenumerate(S):
            if x<0:
                Sname[idx] = ""
                continue
            self.mycursor.execute(req % x)
            res = self.mycursor.fetchone()
            Sname[idx] = res[0]
        S = Sname
        self.printOK()
        self.printDoing("Sorting by ID")
        index =  np.lexsort( (P, S) ) #trier par nom de vendeur puis prix
        #index =  np.lexsort( ( np.tile(self.productsId, (len(S),1)), S) ) # trier par nom de vendeur puis id
        sS = np.take_along_axis(S, index, axis=1)
        sP = np.take_along_axis(P, index, axis=1)
        sSA = np.take_along_axis(SA, index, axis=1)
        self.printOK()
        titles = np.array(self.productsHeadLine)
        autors = np.array(self.productsAutor)
        ids = np.array(self.productsId)
        self.printDoing("Writing in %s"%self.fileout)
        with open(self.fileout, 'w') as f:
            for i, l in enumerate(index):
                print("Best "+str(i+1)+" / Keep in loop :"+str(LINESKEEP), file=f)
                print("  Total : "+str(self.total_cost)+" euros", file=f)
                print("  Time : "+str(self.total_time), file=f)
                Rtable = np.array([ sS[i],sP[i]/100,sSA[i]/100,titles[l],autors[l],ids[l] ]).T
                print(Rtable,file=f)
                print("*"*180,file=f)
            self.printOK()
            self.printDoing("Sorting by Prices")
            index =  np.argsort(P)
            sS = np.take_along_axis(S, index, axis=1)
            sP = np.take_along_axis(P, index, axis=1)
            sSA = np.take_along_axis(SA, index, axis=1)
            self.printOK()
            self.printDoing("Writing in %s"%self.fileout)
            for i, l in enumerate(index):
                Rtable = np.array([sP[i]/100,sSA[i]/100,titles[l],autors[l],sS[i],ids[l]]).T
                print(Rtable,file=f)
                print("*"*180,file=f)
        self.printOK()

    def exec(self):
        if self.update:
            self.productsId = self.whishlist_download()
            #self.msqlShippingFree()
            return
        if self.filein:
            self.productsId = self.get_productsId()
            if self.prices_update:
                self.downloadPrices()
            #self.msqlShippingFree()
        if not self.nocompute :
            self.msqlShippingFree()
            self.msqlSelectPrices()
            S, P, SA, Ltime, cost = self.computeBest()
            self.total_time = timedelta(seconds=round(Ltime[-1] - Ltime[0]))
            self.total_cost = cost[0]/100
            self.printDoing("   Temps total = %s " % self.total_time, end="\n")
            self.printDoing("   Best total = %.2f euros "% (self.total_cost), end="\n")
            self.dispResults(S[:BESTPRINT,:], P[:BESTPRINT,:], SA[:BESTPRINT,:])

def get_parsed_args():
    description=("RakutenPy - Une application pour optimiser "
                "les frais de port en groupant les produits "
                "de votre liste de favoris sur le "
                "site de vente en ligne.")
    parser = argparse.ArgumentParser(description=description,
                                    prog="rakutenPy.py")
    input_type = parser.add_mutually_exclusive_group()
    # parser.add_argument('--userId', type=str, default=None,
    #                     help="Précisez votre identifiant Rakuten.")
    input_type.add_argument("-f", "--input", type=str, default=None,
                        help="Fichier contenant la liste des ID de votre liste de souhait.")
    input_type.add_argument('-w', '--whishlist', action='store_true',
                    help='Récupère votre nouvelle liste de favoris.')
    parser.add_argument('-n', '--no_compute', action='store_true',
                    help='Passer le calcul.')
    parser.add_argument('-u', '--no_prices_update', action='store_true',
                    help='Ne pas récupérer les prix sur le site.')    
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Résultats du calcul.")
    parser.add_argument('-v','--version',action='version',
                        help='Affiche la version du script.',
                        version="RakutenPy v{0}".format(_V_))
    parser.add_argument('-p', '--product', metavar='produitID', type=int, default=None,
                        help="Affiche le produit qui vous intéresse .")
    parser.add_argument('-s', '--seller', metavar='Id_or_login', type=int, default=None,
                        help="Inspecter la boutique de ce vendeur.")
    parser.add_argument('-q','--quiet',action='store_true',
                        help='Afficher seulement le résultat.')
    return parser.parse_args()

#Begining of the script !!!!
if __name__ == "__main__":
    args = get_parsed_args()
    userscript = wishlist_optimiser(args)
    userscript.exec()