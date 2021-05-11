from typing import Optional
import sqlite3
from fastapi import FastAPI, Request, Response, status, HTTPException
from pydantic import BaseModel

app = FastAPI()


@app.on_event("startup")
async def startup():
    app.db_connection = sqlite3.connect("northwind.db")
    app.db_connection.text_factory = lambda b: b.decode(errors="ignore")  # northwind specific


@app.on_event("shutdown")
async def shutdown():
    app.db_connection.close()

# praca domowa

# 4.1

@app.get("/categories", status_code=200)
async def categores():
    app.db_connection.row_factory = sqlite3.Row
    data = app.db_connection.execute('''
    SELECT CategoryID, CategoryName FROM Categories ORDER BY CategoryID
    ''').fetchall()
    return {"categories": [{"id": x['CategoryID'], "name": x["CategoryName"]} for x in data]}

@app.get("/customers", status_code=200)
async def customers():
    app.db_connection.row_factory = sqlite3.Row
    data = app.db_connection.execute('''
    SELECT CustomerID, CompanyName, (COALESCE(Address, '') || ' ' || COALESCE(PostalCode, '') || ' ' || COALESCE(City, '') || ' ' || COALESCE(Country, '')) AS full_address FROM Customers
    ''').fetchall()
    return {"customers": [{"id": f"{x['CustomerID']}", "name": x["CompanyName"], "full_address": (x["full_address"])} for x in data]}


# 4.2

@app.get('/products/{id}', status_code=200)
async def products(id: int):
    app.db_connection.row_factory = sqlite3.Row
    data = app.db_connection.execute("SELECT ProductName FROM Products WHERE ProductID = ?",(id,)).fetchone()
    if data == None:
        raise HTTPException(status_code=404)
    return {"id": id, "name": data['ProductName']}

#4.3

@app.get('/employees', status_code=200)
async def employees(limit: int = -1, offset: int = 0, order: str = 'id'):
    app.db_connection.row_factory = sqlite3.Row
    columns = {'first_name' : 'FirstName', 'last_name' : 'LastName', 'city' : 'City', 'id' : 'EmployeeID'}
    if order not in columns.keys():
        raise HTTPException(status_code=400) 
    order = columns[order]
    data = app.db_connection.execute(f"SELECT EmployeeID, LastName, FirstName, City FROM Employees ORDER BY {order} LIMIT ? OFFSET ?",(limit, offset, )).fetchall()
    return {"employees": [{"id": x['EmployeeID'],"last_name":x['LastName'],"first_name":x['FirstName'],"city":x['City']} for x in data]}

# 4.4

@app.get('/products_extended', status_code=200)
async def products_extended():
    app.db_connection.row_factory = sqlite3.Row
    data = app.db_connection.execute('''
    SELECT Products.ProductID AS id, Products.ProductName AS name, Categories.CategoryName AS category, Suppliers.CompanyName AS supplier FROM Products 
    JOIN Categories ON Products.CategoryID = Categories.CategoryID JOIN Suppliers ON Products.SupplierID = Suppliers.SupplierID ORDER BY Products.ProductID
    ''').fetchall()
    return {"products_extended": [{"id": x['id'], "name": x['name'], "category": x['category'], "supplier": x['supplier']} for x in data]}

# 4.5

@app.get('/products/{id}/orders', status_code=200)
async def products_id_orders(id: int):
    app.db_connection.row_factory = sqlite3.Row # (UnitPrice x Quantity) - (Discount x (UnitPrice x Quantity))
    data = app.db_connection.execute(f'''
    SELECT Products.ProductID, Orders.OrderID AS id, Customers.CompanyName AS customer, [Order Details].Quantity AS quantity, [Order Details].UnitPrice AS unitprice, [Order Details].Discount as discount 
    FROM Products JOIN [Order Details] ON Products.ProductID = [Order Details].ProductID JOIN Orders ON [Order Details].OrderID = Orders.OrderID JOIN Customers ON Orders.CustomerID = Customers.CustomerID WHERE Products.ProductID = {id} ORDER BY Orders.OrderID
    ''').fetchall()
    if data == []:
        raise HTTPException(status_code=404) #
    return {"orders": [{"id": x["id"], "customer": x["customer"], "quantity": x["quantity"], "total_price": round(((x['unitprice'] * x['quantity']) - (x['discount'] * (x['unitprice'] * x['quantity']))), 2)} for x in data]}


# 4.6
@app.post('/categories', status_code=201)
async def categories_post(json_data: dict):
    name = json_data['name']
    cur = app.db_connection.cursor()
    cur.execute('INSERT INTO Categories (CategoryName) VALUES (?)',(name, ))
    app.db_connection.commit()
    id = cur.lastrowid
    return {"id": id, "name": name}

@app.put('/categories/{id}', status_code = 200)
async def category_id_put(json_data: dict, id: int):
    name = json_data['name']
    data = app.db_connection.execute("SELECT CategoryID FROM Categories WHERE CategoryID = ?", (id,)).fetchone()
    if data == None: # kategoria o podanym id nie istnieje
        raise HTTPException(status_code=404)
    app.db_connection.execute("UPDATE Categories SET CategoryName = ? WHERE CategoryID = ?", (name, id,))
    app.db_connection.commit()
    return {"id": id, "name": name}

@app.delete('/categories/{id}', status_code=200)
async def category_id_delete(id: int):
    data = app.db_connection.execute("SELECT CategoryID FROM Categories WHERE CategoryID = ?", (id,)).fetchone()
    if data == None: # kategoria o podanym id nie istnieje
        raise HTTPException(status_code=404)
    app.db_connection.execute('DELETE FROM Categories WHERE CategoryID = ?', (id, ))
    app.db_connection.commit()
    return {"deleted": 1}

