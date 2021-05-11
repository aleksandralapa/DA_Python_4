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

#zad 1
@app.get("/categories")
async def categories():
    app.db_connection.row_factory = sqlite3.Row
    result = app.db_connection.execute("SELECT CategoryID, CategoryName FROM Categories ORDER BY CategoryID").fetchall()
    return {"categories": [{"id": i['CategoryID'], "name": i["CategoryName"]} for i in result]}

@app.get('/customers')
async def customers():
    customers = app.db_connection.execute("SELECT CustomerID, CompanyName, Address, PostalCode, City, Country FROM Customers ORDER BY CAST(CustomerID as INTEGER)").fetchall()
    response.status_code = 200
    adress = ['']*len(customers)
    index = 0
    for i in customers:
        i = list(i)
        for j in range(2, 6):
            if i[j] is None :
                i[j] = ''
            elif j<5:
                i[j]=i[j]+' '
            adress[index] = adress[index] + i[j]

        index += 1

    return {"customers": [{"id": customers[i][0], "name": customers[i][1], "full_adress": adress[i]} for i in range(len(customers))]}

#zad 2
@app.get('/products/{id}')
async def products_id(id: int):
    app.db_connection.row_factory = sqlite3.Row
    result = app.db_connection.execute("SELECT ProductName FROM Products WHERE ProductID = ?",(id,)).fetchone()
    if result is None:
        raise HTTPException(status_code=404)
    return {"id": id, "name": result['ProductName']}

#zad 3
@app.get('/employees')
async def employees(limit: int, offset: int, order: str = 'id'):
    app.db_connection.row_factory = sqlite3.Row
    if order == 'id':
        order = 'EmployeeID'
    elif order == 'city':
        order = 'City'
    elif order == 'last_name':
        order = 'LastName'
    elif order == 'first_name':
        order = 'FirstName'
    else:
        raise HTTPException(status_code=400)
    result = app.db_connection.execute(f"SELECT EmployeeID, LastName, FirstName, City FROM Employees ORDER BY {order} LIMIT ? OFFSET ?",(limit, offset, )).fetchall()
    return {"employees": [{"id": i['EmployeeID'],"last_name":i['LastName'],"first_name":i['FirstName'],"city":i['City']} for i in result]}

#zad 4
@app.get('/products_extended')
async def products_extra():
    app.db_connection.row_factory = sqlite3.Row
    result = app.db_connection.execute("SELECT Products.ProductID, Products.ProductName, Categories.CategoryName, Suppliers.CompanyName FROM Products  JOIN Categories ON Products.CategoryID = Categories.CategoryID JOIN Suppliers ON Products.SupplierID = Suppliers.SupplierID ORDER BY Products.ProductID").fetchall()
    return {"products_extended": [{"id": i['ProductID'], "name": i['ProductName'], "category": i['CategoryName'], "supplier": i['CompanyName']} for i in result]}

#zad 5
@app.get('/products/{id}/orders')
async def products_id_orders(id: int):
    app.db_connection.row_factory = sqlite3.Row
    result = app.db_connection.execute(f"""SELECT Orders.OrderID, Customers.CompanyName, [Order Details].Quantity, [Order Details].UnitPrice, [Order Details].Discount FROM Products 
    JOIN [Order Details] ON Products.ProductID = [Order Details].ProductID JOIN Orders ON [Order Details].OrderID = Orders.OrderID JOIN Customers ON Orders.CustomerID = Customers.CustomerID 
    WHERE Products.ProductID = {id} ORDER BY Orders.OrderID""").fetchall()
    if result is None:
        raise HTTPException(status_code=404)
    price = [0]*len(result)
    index = 0
    for i in result:
        price[index] = round((i['UnitPrice'] * i['Quantity']) - (i['Discount'] * i['UnitPrice'] * i['Quantity']), 2)
        index += 1
    return {"orders": [{"id": result[i]["OrderID"], "customer": result[i]["CompanyName"], "quantity": result[i]["quantity"], "total_price": price[i]} for i in range(len(result))]}

#zad 6
@app.post('/categories', status_code=201)
async def p_categories(data: dict):
    app.db_connection.row_factory = sqlite3.Row
    result = app.db_connection.execute("SELECT * FROM Categories").fetchall()
    name = data['name']
    c = app.db_connection.cursor()
    c.execute(f'ALTER TABLE Categories ADD COLUMN {name}')
    app.db_connection.commit()
    id = len(result)+1
    return {"id": id, "name": name}

