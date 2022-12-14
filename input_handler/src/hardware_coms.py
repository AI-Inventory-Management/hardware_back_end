"""
Handler.

    Cloud handler of frontend communications.
    
Classes:
    DbUploader
    
Functions:
    home() -> Rendered Template
    
    constant_messages() -> dict
    
    initialization_messages() -> dict
    
Author:
    Jose Angel del Angel
    
"""
#_________________________________Libraries____________________________________
from flask import Flask, render_template, request, jsonify
import mysql.connector
import handler_keys
import pandas as pd

from Decrypter import Decrypter

#__________________________________Classes_____________________________________
class DbUploader():
    """
    Handler.
    
        Handles information recieved from frontend and uploads information to 
        database.
        
    Attributes
    ----------
    db_connection : mysql Connection
        Connection.
    
    db_cursor : mysql Cursor
        Cursor.
    
    decrypter : Decrypter
        Decrypter.
        
    Methods
    -------
    close_db_connection():
        Close connection to database.
        
    open_db_connection():
        Open connection to database.
        
    decypher(message):
        Decypher given message.
        
    parse_query_result(result_columns):
        Format into dataframe.
    
    two_cols_df_to_dict(df, key_column_name, value_column_name):
        Transform dataframe to dictionary.
    
    create_field_equals_or_chain_query(field:str, values_to_equal:list):
        Chain queries with ORs.
        
    fetch_product_ids(products:list):
        Get id of product from database.
    
    fetch_prev_stock(store_id):
        Get stock of id from database.
        
    fetch_min_stock(store_id):
        Get minimum stock of id from database.
        
    fetch_max_stock(store_id):
        Get maximum stock of id from database.
        
    fetch_store_id(store_name, store_status, store_latitude, store_longitude, store_state, store_municipality, store_zip_code, store_address)
        Get store id given de store information.
        
    fetch_store_status(store_id):
        Get status of id from database.
    
    register_new_store(store_name, store_status, store_latitude, store_longitude, store_state, store_municipality, store_zip_code, store_address)
        Register new store with the given information.
    
    register_new_inventory(product_id, store_id, stock, min_stock, max_stock):
        Register new inventory with the given data.
        
    register_new_sale(product_id, store_id, timestamp):
        Register new sale with the given information.
        
    create_notification(id_store, new_status):
        Register notification with the given information.
        
    update_inventory(store_id, product_id, new_stock):
        Update inventory of store.
        
    update_store_status(store_id, status):
        Update status of store.
    
    handle_change_on_status(store_id, mins_stock, maxs_stock, stocks):
        Change status if necessary.
    
    handle_cahanges_on_store_products(prev:dict, curr:dict, id_store:str):
        Change store products if necessary.
        
    handle_changes_on_store_stock(prev, curr, id_store:str, timestamp:str):
        Change store stock if necessary.
        
    handle_constant_message(message):
        Recieve and handle constant messages.
        
    handle_initialization_message(message):
        Recieve and handle initialization messages.
    
    """
    
    def __init__(self) -> None:
        """
        Construct attributes of the class.

        Returns
        -------
        None
            DESCRIPTION.

        """
        self.db_connection = None
        self.db_cursor = None
        self.decrypter = Decrypter()
        
    def close_db_connection(self):
        """
        Close db connection.

        Returns
        -------
        None.

        """
        self.db_cursor.close()
        self.db_connection.close()

    def open_db_connection(self):
        """
        Open db connection.

        Returns
        -------
        None.

        """
        self.db_connection = mysql.connector.connect(host=handler_keys.DB_HOST, user=handler_keys.DB_USER, passwd=handler_keys.DB_PASSWORD, database=handler_keys.DB_NAME)
        self.db_cursor = self.db_connection.cursor()

    # =============================== HELPERS ===============================
    
    def decypher(self, message):
        """
        Decyoher message.

        Parameters
        ----------
        message : dict
            Message to decypher.

        Returns
        -------
        dict
            Message dechyphered.

        """
        return self.decrypter.decrypt(message)
    
    def parse_query_result(self, result_columns):
        """
        Buil dataframe.

        Parameters
        ----------
        result_columns : list
            Columns expected.

        Returns
        -------
        df : pandas dataframe
            Dataframe.

        """
        df= pd.DataFrame(columns = result_columns)
        for register in self.db_cursor:
            data = {}
            for i in range(len(result_columns)):
                data[result_columns[i]] = register[i]
            new_row = pd.DataFrame(data, index=[0])
            df = pd.concat([df, new_row]).reset_index(drop=True)
        return df

    def two_cols_df_to_dict(self, df, key_column_name, value_column_name):
        """
        Return dataframe as dictionary.

        Parameters
        ----------
        df : pandas Dataframe
            Dataframe.
            
        key_column_name : string
            Name of key.
            
        value_column_name : string
            Value.

        Returns
        -------
        new_dict : dict
            Dictionary.

        """
        new_dict = {}
        for i in df.index:
            new_dict[df[key_column_name][i]] = df[value_column_name][i]        
        return new_dict

    def create_field_equals_or_chain_query(self, field:str, values_to_equal:list):
        """
        Build query.
        
            This method generates a chain of ORs to query a field with several 
            values, i.e. 'Inventory.id_product = val1 OR Inventory.id_product = 
            val2 OR Inventory.id_product = val3' 

        Parameters
        ----------
        field : str
            Field to fill.
            
        values_to_equal : list
            Values to fill with.

        Returns
        -------
        result : string
            Query.

        """
        result = ""
        values_len = len(values_to_equal)
        for i in range(values_len):
            result += field + "=" + "'{v}'".format(v=values_to_equal[i])
            if i < values_len-1:
                result += " OR "
        return result

    # =============================== FETCH DATA FROM DB ===============================

    def fetch_product_ids(self, products:list)->dict:
        """
        Get products ids from database.

        Parameters
        ----------
        products : list
            Products to get id from.

        Returns
        -------
        dict
            Products to ids dictionary.

        """
        products_query = self.create_field_equals_or_chain_query("Product.ean", products)
        product_ids_query = "SELECT Product.ean, Product.id_product FROM Product WHERE {q}".format(q = products_query)

        self.open_db_connection()
        self.db_cursor.execute(product_ids_query)
        product_ids_result = self.parse_query_result(result_columns=["product_ean", "product_id"])
        product_ids_result = self.two_cols_df_to_dict(product_ids_result, "product_ean", "product_id")
        self.close_db_connection()
        return product_ids_result

    def fetch_prev_stock(self, store_id):
        """
        Get previous stock of store from database.

        Parameters
        ----------
        store_id : string
            Id of store.

        Returns
        -------
        result : dict
            Previous stock.

        """
        self.open_db_connection()
        fetch_stock_query = """SELECT Product.ean, Inventory.stock
        FROM Inventory 
        INNER JOIN Product ON Inventory.id_product=Product.id_product 
        WHERE Inventory.id_store = '{store_id}'
        """.format(store_id = store_id)
        self.db_cursor.execute(fetch_stock_query)
        result = self.parse_query_result(result_columns=["product_ean", "product_stock"])
        result = self.two_cols_df_to_dict(result, "product_ean", "product_stock")
        self.close_db_connection()
        return result
    
    def fetch_min_stock(self, store_id):
        """
        Get minimum stock of store.

        Parameters
        ----------
        store_id : string
            Id of store.

        Returns
        -------
        result : dict
            Minimum stock of store.

        """
        self.open_db_connection()
        fetch_min_stock_query = """SELECT Product.ean, Inventory.min_stock
        FROM Inventory
        INNER JOIN Product ON Inventory.id_product = Product.id_product
        WHERE Inventory.id_store = '{store_id}'
        """.format(store_id = store_id)
        self.db_cursor.execute(fetch_min_stock_query)
        result = self.parse_query_result(result_columns=["product_ean", "product_min_stock"])
        result = self.two_cols_df_to_dict(result, "product_ean", "product_min_stock")
        self.close_db_connection()
        return result
        
    def fetch_max_stock(self, store_id):
        """
        Get maximum stock of store.

        Parameters
        ----------
        store_id : string
            Id of store.

        Returns
        -------
        result : dict
            Maximum stock of store.

        """
        self.open_db_connection()
        fetch_max_stock_query = """SELECT Product.ean, Inventory.max_stock
        FROM Inventory
        INNER JOIN Product ON Inventory.id_product = Product.id_product
        WHERE Inventory.id_store = '{store_id}'
        """.format(store_id = store_id)
        self.db_cursor.execute(fetch_max_stock_query)
        result = self.parse_query_result(result_columns=["product_ean", "product_min_stock"])
        result = self.two_cols_df_to_dict(result, "product_ean", "product_min_stock")
        self.close_db_connection()
        return result
        
    
    def fetch_store_id(self, store_name, store_status, store_latitude, store_longitude, store_state, store_municipality, store_zip_code, store_address):        
        """
        Get id of store given the information of the store.

        Parameters
        ----------
        store_name : string
            Name of store.
            
        store_status : string
            Status of store.
            
        store_latitude : string
            Latitude of store.
            
        store_longitude : string
            Longitude of store.
            
        store_state : string
            State of store.
            
        store_municipality : string
            Municipality of store.
            
        store_zip_code : string
            Zip code of store.
            
        store_address : string
            Address of store.

        Returns
        -------
        store_ids_result : string
            Id of store.

        """
        store_id_query = """SELECT Store.name, Store.id_store 
            FROM Store 
            WHERE (Store.name='{store_name}' AND
            Store.status={store_status} AND
            Store.latitude >= {store_latitude}-0.0001 AND Store.latitude <= {store_latitude}+0.0001 AND
            Store.longitude >= {store_longitude}-0.0001 AND Store.longitude <= {store_longitude}+0.0001 AND
            Store.state='{store_state}' AND
            Store.municipality='{store_municipality}' AND
            Store.zip_code={store_zip_code} AND
            Store.address='{store_address}')
            """.format(
                store_name=store_name,
                store_status=store_status,
                store_latitude=store_latitude,
                store_longitude=store_longitude,
                store_state=store_state,
                store_municipality=store_municipality,
                store_zip_code=store_zip_code,
                store_address=store_address)
        self.open_db_connection()
        self.db_cursor.execute(store_id_query)
        store_ids_result = self.parse_query_result(result_columns=["store_name", "store_id"])
        print("store id query is:")
        print(store_id_query)
        store_ids_result = self.two_cols_df_to_dict(store_ids_result, "store_name", "store_id")
        self.close_db_connection()
        return store_ids_result
    
    def fetch_store_status(self, store_id):
        """
        Get store status from store.

        Parameters
        ----------
        store_id : string
            Id of store.

        Returns
        -------
        int
            Status.

        """
        store_status_query = """SELECT Store.name, Store.status FROM Store 
                                WHERE Store.id_store = {store_id}""".format(
                                store_id = store_id)
        self.open_db_connection()
        self.db_cursor.execute(store_status_query)
        store_status = self.parse_query_result(result_columns = ["store_name", 
                                                                 "store_status"])
        store_status_result = self.two_cols_df_to_dict(store_status, 
                                                       "store_name", "store_status")
        self.close_db_connection()
        try:
            return list(store_status_result.values())[0]
        except IndexError:
            # return a default status
            return 2
        
        
    # =============================== GENERATE NEW REGISTERS ON DB ===============================

    def register_new_store(self, store_name, store_status, store_latitude, store_longitude, store_state, store_municipality, store_zip_code, store_address):        
        """
        Register new store on database.

        Parameters
        ----------
        store_name : string
            Name of store.
            
        store_status : string
            Status of store.
            
        store_latitude : string
            Latitude of store.
            
        store_longitude : string
            Longitude of store.
            
        store_state : string
            State of store.
            
        store_municipality : string
            Municipality of store.
            
        store_zip_code : string
            Zip code of store.
            
        store_address : string
            Address of store.

        Returns
        -------
        None.

        """
        register_store_query = """INSERT INTO Store(name, status, latitude, longitude, state, municipality, zip_code, address)
            VALUES ('{store_name}', {store_status}, {store_latitude}, {store_longitude}, '{store_state}', '{store_municipality}', {store_zip_code}, '{store_address}')
            """.format(
            store_name=store_name,
            store_status=store_status,
            store_latitude=store_latitude,
            store_longitude=store_longitude,
            store_state=store_state,
            store_municipality=store_municipality,
            store_zip_code=store_zip_code,
            store_address=store_address)
        self.open_db_connection()
        self.db_cursor.execute(register_store_query)
        self.db_connection.commit()
        self.close_db_connection()

    def register_new_inventory(self, product_id, store_id, stock, min_stock, max_stock):
        """
        Register new inventory of store.

        Parameters
        ----------
        product_id : string
            Id of product.
            
        store_id : string
            Id of store.
            
        stock : int
            Actual stock of product.
            
        min_stock : int
            Minimum stock of product.
            
        max_stock : int
            Maximum stock of product.

        Returns
        -------
        None.

        """
        inventary_creation_query = """INSERT INTO Inventory(id_product, id_store, stock, min_stock, max_stock)
            VALUES ({product_id}, {store_id}, {stock}, {min_stock}, {max_stock})""".format(
            product_id=product_id,
            store_id=store_id,
            stock=stock,
            min_stock=min_stock,
            max_stock=max_stock)        
        self.open_db_connection()
        self.db_cursor.execute(inventary_creation_query)
        self.db_connection.commit()
        self.close_db_connection()

    def register_new_sale(self, product_id, store_id, timestamp):
        """
        Register new sale of product in store.

        Parameters
        ----------
        product_id : string
            Id of product.
            
        store_id : string
            Id of store.
            
        timestamp : long
            Timestamp.

        Returns
        -------
        None.

        """
        register_sale_query = "INSERT INTO Sale(id_product, id_store, timestamp) VALUES ({product_id}, {store_id}, {t})".format(product_id=product_id, store_id=store_id, t=timestamp)
        self.open_db_connection()
        self.db_cursor.execute(register_sale_query)
        self.db_connection.commit()
        self.close_db_connection()
    
    def create_notification(self, id_store, new_status):
        """
        Register notification in database.

        Parameters
        ----------
        id_store : string
            Id of store.
            
        new_status : int
            New status.

        Returns
        -------
        None.

        """
        notification_creation_query = """INSERT INTO Notification(id_store, new_status)
                                         VALUES({id_store}, {new_status})""".format(
                                             id_store = id_store, 
                                             new_status = new_status)
        self.open_db_connection()
        self.db_cursor.execute(notification_creation_query)
        self.db_connection.commit()
        self.close_db_connection()
    
    # =============================== UPDATE REGISTERS ON DB ===============================

    def update_inventory(self, store_id, product_id, new_stock):
        """
        Update inventory of store in database.

        Parameters
        ----------
        store_id : string
            Id of store.
            
        product_id : string
            Id of product.
            
        new_stock : int
            New stock.

        Returns
        -------
        None.

        """
        inventory_update_query = "UPDATE Inventory SET Inventory.stock={new_stock} WHERE Inventory.id_store={store_id} AND Inventory.id_product={product_id}".format(new_stock=new_stock, store_id=store_id, product_id=product_id)
        self.open_db_connection()
        self.db_cursor.execute(inventory_update_query)
        self.db_connection.commit()
        self.close_db_connection()

    def update_store_status(self, store_id, status):
        """
        Update store status.

        Parameters
        ----------
        store_id : string
            Id of store.
            
        status : int
            New status.

        Returns
        -------
        None.

        """
        status_update_query = "UPDATE Store SET Store.status = {status} WHERE Store.id_store = {store_id}".format(status = status, store_id = store_id)
        self.open_db_connection()
        self.db_cursor.execute(status_update_query)
        self.db_connection.commit()
        self.close_db_connection()
    
    # =============================== MAIN HANDLERS ===============================
    
    def handle_change_on_status(self, store_id, mins_stock, maxs_stock, stocks):
        """
        Handle any change on status.

        Parameters
        ----------
        store_id : string
            If of store.
            
        mins_stock : list
            Minimum stocks.
            
        maxs_stock : list
            Maximum stocks.
            
        stocks : list
            Stocks.

        Returns
        -------
        None.

        """
        curr_store_status = self.fetch_store_status(store_id)
        
        norm = []
        
        current_products = list(stocks.keys())
        current_products = [i for i in current_products if i not in ["0"]] # we delete empty spaces from curent products
        for label in current_products:
            if not (maxs_stock[label] - mins_stock[label]) == 0:
                norm.append((stocks[label] - mins_stock[label])/(maxs_stock[label] - mins_stock[label]))
        
        if len(norm) > 0:
            mean = sum(norm) / len(norm)
        else:
            mean = 0
        
        if mean > 0.75:
            new_status = 1
        elif mean > 0.25:
            new_status = 2
        else:
            new_status = 3
        
        if curr_store_status != new_status:
            self.update_store_status(store_id = store_id, status = new_status)
            self.create_notification(id_store = store_id,
                                     new_status = new_status)
        
    def handle_cahanges_on_store_products(self, prev:dict, curr:dict, id_store:str):
        """
        Handle any changes on store products.

        Parameters
        ----------
        prev : dict
            Previous stocks.
            
        curr : dict
            Current stocks.
            
        id_store : str
            Id of store.

        Returns
        -------
        None.

        """
        # check if new products exist on the new input and if
        # they do we create a new inventory table for each with defaul max stock 
        # and min stock values
        products_only_in_curr = [ product for product in curr.keys() if product not in prev.keys() ]
        products_only_in_curr = [i for i in products_only_in_curr if i not in ["0"]] # we delete empty spaces from curent products
        if len(products_only_in_curr) == 0:
            return
        # if there are products in current stock that are not in prev stock we fetch the product ids for each        
        product_ids_result = self.fetch_product_ids(products_only_in_curr)

        # Once we fetch the product ids of products only in current stock we create a new inventary for each of this products

        for product in products_only_in_curr:
            print("creating new inventary register for {p}".format(p=product))
            self.register_new_inventory(product_ids_result[product], id_store, curr[product], 0, 0) 
            # TODO: change the min max stock args to non existing product flag       
    
    def handle_changes_on_store_stock(self, prev, curr, id_store:str, timestamp:str):
        """
        Handle any change on store stock

        Parameters
        ----------
        prev : dict
            Previous stocks.
            
        curr : dict
            Current stocks.
            
        id_store : str
            Id of store.
            
        timestamp : str
            Timestamp.

        Returns
        -------
        None.

        """
        current_products = list(curr.keys())
        current_products = [i for i in current_products if i not in ["0"]] # we delete empty spaces from curent products 
        product_ids = self.fetch_product_ids(current_products)
        
        for product in current_products:
            prev_vs_curr_stock = prev[product] - curr[product]
            product_id = product_ids[product]
            product_stock = curr[product]
            
            
            if prev_vs_curr_stock > 0:
                # update inventory
                print("updating inventory for {p}".format(p=product))
                self.update_inventory(store_id=id_store, product_id=product_id, new_stock=product_stock)
                #register sale
                print("registering a sell for {p}".format(p=product))
                self.register_new_sale(product_id=product_id, store_id=id_store, timestamp=timestamp)
            elif prev_vs_curr_stock < 0:
                # update inventory                
                print("updating inventory for {p}".format(p=product))
                self.update_inventory(store_id=id_store, product_id=product_id, new_stock=product_stock) 
        

    def handle_constant_message(self, message): 
        """
        Handle any constant messages.

        Parameters
        ----------
        message : dict
            Message.

        Returns
        -------
        None.

        """
        message = self.decypher(message)
        print(message)
        prev_stock = self.fetch_prev_stock(store_id=message['store_id'])
        self.handle_cahanges_on_store_products(prev_stock, message['content_count'], message['store_id'])
        prev_stock = self.fetch_prev_stock(store_id=message['store_id'])
        self.handle_changes_on_store_stock(prev_stock, message['content_count'], message['store_id'], message['timestamp'])
        mins = self.fetch_min_stock(store_id = message["store_id"])
        maxs = self.fetch_max_stock(store_id = message["store_id"])
        self.handle_change_on_status(store_id = message["store_id"], mins_stock = mins, maxs_stock = maxs, stocks = message["content_count"])
        
    def handle_initialization_message(self, message):
        """
        Handle initialization message.

        Parameters
        ----------
        message : dict
            Message.

        Returns
        -------
        store_id : string
            Id of store.

        """
        message = self.decypher(message)
        print(message)
        self.register_new_store(message["store_name"], 1, message["store_latitude"], message["store_longitude"], message["store_state"], message["store_municipality"], message["store_zip_code"], message["store_address"])
        store_products = list(message["store_curr_stock"].keys())
        store_products_ids = self.fetch_product_ids(store_products)
        store_id = self.fetch_store_id(message["store_name"], 1, message["store_latitude"], message["store_longitude"], message["store_state"], message["store_municipality"], message["store_zip_code"], message["store_address"])
        store_id = store_id[message["store_name"]]
        print("fetch store_id result is:")
        print(store_products_ids)
        print("fetch store id result is:")
        print(store_id)
        for product in store_products:
            self.register_new_inventory(store_products_ids[product], store_id, message["store_curr_stock"][product], message["store_min_stocks"][product], message["store_max_stocks"][product])
        return store_id

#_________________________________Variables____________________________________
app = Flask(__name__)
app.secret_key = handler_keys.FLASK_APP_KEY

uploader = DbUploader()

#_________________________________Functions____________________________________
@app.route('/', methods = ['GET'])
def home():
    """
    Render home template.

    Returns
    -------
    Rendered Template
        Rednered home template.

    """
    return render_template('home_template.html')

@app.route('/constant_messages', methods=['GET', 'POST'])
def constant_messages():    
    """
    Recieve constant messages.

    Returns
    -------
    dict
        Dictionary.

    """
    content = request.json    
    uploader.handle_constant_message(content)
    print("========================================================")
    print("printing data fetched on server:")
    print(content)
    print("")
    return jsonify({})

@app.route('/initaialization_messages', methods=['GET', 'POST'])
def initialization_messages():
    """
    Recieve initialization messages.

    Returns
    -------
    dict
        Dictionary.

    """
    content = request.json    
    store_id = uploader.handle_initialization_message(content)
    print("========================================================")
    print("printing data fetched on server:")
    print(content)
    print("")
    return jsonify({"store_id":store_id})

#____________________________________Main______________________________________
if __name__ == '__main__':
	app.run(host = '0.0.0.0', port = 7000, debug = True)