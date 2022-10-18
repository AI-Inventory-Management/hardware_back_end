from datetime import datetime
import cv2 as cv
import numpy as np
import os
from threading import Event
import requests
import json

class InitializationMessageUploader():
    def __init__(self):        
        self.message = {}
        self.severs_handler_endpoint = "http://192.168.195.106:7000/initaialization_messages"
        '''
        self.soda_labels = [
            'fresca lata 355 ml',
            'sidral mundet lata 355 ml',
            'fresca botella de plastico 600 ml',
            'fuze tea durazno 600 ml',
            'power ade mora azul botella de plastico 500 ml',
            'delaware punch lata 355 ml',
            'vacio',
            'del valle durazno botella de vidrio 413 ml',
            'sidral mundet botella de plastico 600 ml',
            'coca cola botella de plastico 600 ml',
            'power ade mora azul lata 453 ml',
            'coca cola lata 355 ml']
        '''    
        self.soda_labels = ['fresca lata 355 ml', 'sidral mundet lata 355 ml']
        self.store_id = ""
    
    def build_message(self, 
        store_name:str, 
        store_latitude:float,
        store_longitude:float,
        store_state:str,
        store_municipality:str,
        store_zip_code:int,
        store_address:str,
        store_curr_stock:dict, 
        store_min_stocks:dict,
        store_max_stocks:dict):      
          
        self.message["store_name"] = store_name
        self.message["store_latitude"] = store_latitude
        self.message["store_longitude"] = store_longitude
        self.message["store_state"] = store_state
        self.message["store_municipality"] = store_municipality
        self.message["store_zip_code"] = store_zip_code
        self.message["store_address"] = store_address
        self.message["store_curr_stock"] = store_curr_stock
        self.message["store_min_stocks"] = store_min_stocks
        self.message["store_max_stocks"] = store_max_stocks
        
    def obtain_store_data(self):
        try:
            with open("./data/store_data.json", 'r') as f:
                data = json.load(f)
                f.close()
                if len(data) == 0:
                    store_name = input("please write the NAME of the new sotre: ")
                    store_latitude = float(input("please write the LATITUDE of the new sotre: "))
                    store_longitude = float(input("please write the LONGITUDE of the new sotre: "))
                    store_state = input("please write the STATE of the new sotre: ")
                    store_municipality = input("please write the MUNICIPALITY of the new sotre: ")
                    store_zip_code = int(input("please write the ZIP_CODE of the new sotre: "))
                    store_address = input("please write the ADDRESS of the new sotre: ")
            
                    print("Thanks, please input the store stock details")
                    current_stock = {}        
                    min_stocks = {}
                    max_stocks = {}
                    for soda in self.soda_labels:
                        print('')
                        print('-------------- {s} --------------'.format(s=soda))
                        current_stock[soda] = int(input("how many {s} are on the store right now: ".format(s=soda)))
                        min_stocks[soda] = int(input("whats the min of {s} to generate an alert: ".format(s=soda)))
                        max_stocks[soda] = int(input("whats amount of {s} to fill the store: ".format(s=soda)))
                else:
                    return False
                    
            self.build_message(store_name, store_latitude, store_longitude, store_state, store_municipality, store_zip_code, store_address, current_stock, min_stocks, max_stocks)
            return True
        except:
            store_name = input("please write the NAME of the new store: ")
            store_latitude = float(input("please write the LATITUDE of the new sotre: "))
            store_longitude = float(input("please write the LONGITUDE of the new sotre: "))
            store_state = input("please write the STATE of the new sotre: ")
            store_municipality = input("please write the MUNICIPALITY of the new sotre: ")
            store_zip_code = int(input("please write the ZIP_CODE of the new sotre: "))
            store_address = input("please write the ADDRESS of the new sotre: ")
    
            print("Thanks, please input the store stock details")
            current_stock = {}        
            min_stocks = {}
            max_stocks = {}
            for soda in self.soda_labels:
                print('')
                print('-------------- {s} --------------'.format(s=soda))
                current_stock[soda] = int(input("how many {s} are on the store right nosw: ".format(s=soda)))
                min_stocks[soda] = int(input("whats the min of {s} to generate an alert: ".format(s=soda)))
                max_stocks[soda] = int(input("whats amount of {s} to fill the store: ".format(s=soda)))
            
            self.build_message(store_name, store_latitude, store_longitude, store_state, store_municipality, store_zip_code, store_address, current_stock, min_stocks, max_stocks)
            return True
        
    def build_data_file(self, verbose = False):
        data = {"store_id" : self.store_id, "store_info" : self.message}
        with open("./data/store_data.json", 'w') as f:
            json.dump(data, f)
            f.close
            if verbose:
                print("Store information saved succesfully.")
    
    def upload_message(self, verbose = False):
        res = requests.post(self.severs_handler_endpoint, json=self.message)
        if res.ok and verbose:
            print("data sended to server succesfully")
        
        self.store_id = str(res.json()["store_id"])
        if verbose:
            print (self.store_id)
            
    def build_return_test_message(self):
        self.build_message(store_name = "as", 
                           store_latitude = 1, 
                           store_longitude= 1, 
                           store_state = 1, 
                           store_municipality = 1, 
                           store_zip_code = 1, 
                           store_address = 1, 
                           store_curr_stock = {"a" : 1}, 
                           store_min_stocks = {"a" : 1}, 
                           store_max_stocks = {"a" : 1})
    
    def upload_test_mesage(self):
        store_name = input("please write the NAME of the new store: ")
        store_latitude = float(input("please write the LATITUDE of the new sotre: "))
        store_longitude = float(input("please write the LONGITUDE of the new sotre: "))
        store_state = input("please write the STATE of the new sotre: ")
        store_municipality = input("please write the MUNICIPALITY of the new sotre: ")
        store_zip_code = int(input("please write the ZIP_CODE of the new sotre: "))
        store_address = input("please write the ADDRESS of the new sotre: ")

        print("Thanks, please input the store stock details")
        current_stock = {}        
        min_stocks = {}
        max_stocks = {}
        for soda in self.soda_labels:
            print('')
            print('-------------- {s} --------------'.format(s=soda))
            current_stock[soda] = int(input("how many {s} are on the store right now: ".format(s=soda)))
            min_stocks[soda] = int(input("whats the min of {s} to generate an alert: ".format(s=soda)))
            max_stocks[soda] = int(input("whats amount of {s} to fill the store: ".format(s=soda)))
        self.build_message(store_name, store_latitude, store_longitude, store_state, store_municipality, store_zip_code, store_address, current_stock, min_stocks, max_stocks)
        self.upload_message(verbose=True)
        
    def run_comms_demo (self):
        self.build_return_test_message()
        self.upload_message(verbose = True)
        self.build_data_file(verbose = True)
        
if __name__ == "__main__":
    uploader = InitializationMessageUploader()
    uploader.run_comms_demo()