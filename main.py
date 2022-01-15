#!/usr/bin/python

import sys
import sqlite3
import atexit
from operator import attrgetter

# Data Transfer Object
class Hat:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity

class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name 

class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat

class Output:
    def __init__(self, topping, supplierName, location):
        self.topping = topping
        self.supplierName = supplierName
        self.location = location 

# Data Access Object
class _Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("INSERT INTO hats (id, topping, supplier, quantity) VALUES (?,?,?,?)", [hat.id, hat.topping, hat.supplier, hat.quantity])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("SELECT id, topping, supplier, quantity FROM hats WHERE id = ?", [id])
        return Hat(*c.fetchone())

    def findByTopping(self, topping, supplier):
        c = self._conn.cursor()
        c.execute("SELECT id, topping, supplier, quantity FROM hats WHERE topping = ? and supplier = ?", [topping, supplier])
        return Hat(*c.fetchone())

    def remove(self, id):
        c = self._conn.cursor()
        c.execute("DELETE FROM hats WHERE hats.id = ?", [id])

    def update(self, grade):
        c = self._conn.cursor()
        c.execute("UPDATE hats SET quantity=(?) WHERE hats.id = ?", [grade.quantity, grade.id])

    def findall(self):
        c = self._conn.cursor()
        all = c.execute('SELECT * FROM hats').fetchall()
        return [Hat(*row) for row in all]

class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("INSERT INTO suppliers (id, name) VALUES (?,?)", [supplier.id, supplier.name])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("SELECT id, name FROM suppliers WHERE id = ?", [id])
        return Supplier(*c.fetchone())

    def findall(self):
        c = self._conn.cursor()
        all = c.execute('SELECT * FROM suppliers').fetchall()
        return [Supplier(*row) for row in all]


class _Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, order):
        self._conn.execute("INSERT INTO orders (id, location, hat) VALUES (?,?,?)", [order.id, order.location, order.hat])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("SELECT id, location, hat FROM orders WHERE id = ?", [id])
        return Order(*c.fetchone())

    def findall(self):
        c = self._conn.cursor()
        all = c.execute('SELECT * FROM orders').fetchall()
        return [Order(*row) for row in all]

# Reposetory
class _Reposetory:
    def __init__(self, dbPath):
        self._conn = sqlite3.connect(dbPath)
        self.hats = _Hats(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.orders = _Orders(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def createTables(self):
        self._conn.executescript("""
            CREATE TABLE suppliers (
                id          INTEGER     NOT NULL,
                name        STRING      NOT NULL, 

                PRIMARY KEY (id)
            )
        """)
                # CREATE INDEX idx_suppliers_id ON suppliers(id)

        self._conn.executescript("""
            CREATE TABLE hats (
                id          INTEGER     NOT NULL,
                topping     STRING      NOT NULL,
                supplier    INTEGER     NOT NULL,
                quantity    INTEGER     NOT NULL,

                PRIMARY KEY (id),

                FOREIGN KEY (supplier) REFERENCES suppliers(id)
            );
        """)

                # CREATE INDEX (idx_hats_topping) ON hats(topping),
                # CREATE INDEX idx_hats_id ON hats(id),

        self._conn.executescript("""
            CREATE TABLE orders (
                id          INTEGER     NOT NULL,
                location    STRING      NOT NULL,
                hat         INTEGER     NOT NULL, 

                PRIMARY KEY (id),

                FOREIGN KEY (hat) REFERENCES hats(id)
            );
        """)
                # CREATE INDEX idx_orders_id ON orders(id),
        
    def buildPizzeria(self, configPath): 
        
        # Read config file.
        with open(configPath) as f:

            # First line = <hats>,<suppliers>
            line = f.readline()
            sizes = line.split(',')

            for i in range(int(sizes[0])):
                self.hats.insert(Hat(*(f.readline().rstrip('\n').split(','))))

            for i in range(int(sizes[1])):
                self.suppliers.insert(Supplier(*(f.readline().rstrip('\n').split(','))))

    def getSuppliers(self, topping):
        c = self._conn.cursor()

        # Get all the suppliers that can make the desired topping
        arr = c.execute("""
            SELECT suppliers.id, suppliers.name 
            FROM suppliers INNER JOIN hats ON hats.supplier = suppliers.id
            WHERE hats.topping = ?""", [topping]).fetchall()
        return [Supplier(*s) for s in arr]

    def orderPizza(self, order):
        
        # orderring
        self.orders.insert(order)

        # Create output for this order.
        output = self.__createOutput(order)

        # Update quantity and delete hat if needed.
        hat = self.hats.find(order.hat)
        hat.quantity -= 1
        if (hat.quantity == 0):
            self.hats.remove(hat.id)
        else:
            self.hats.update(hat)

        return output

    def __createOutput(self, order):
        c = self._conn.cursor()

        # Get all the suppliers that can make the desired topping
        arr = c.execute("""
            SELECT hats.topping, suppliers.name, orders.location
            FROM orders 
            INNER JOIN hats 
                ON hats.id = orders.hat
            INNER JOIN suppliers 
                ON suppliers.id = hats.supplier
            WHERE orders.id = ?""", [order.id]).fetchall()
        
        outs = [Output(*s) for s in arr]

        if(len(outs) != 1):
            print("error")
            return None
        return outs[0]

def main():

    # Parse input
    if (len(sys.argv) != 5):
        print("Wrong number of arguments ", len(sys.argv))
        exit(1)
    configPath = sys.argv[1]
    ordersPath = sys.argv[2]
    outputPath = sys.argv[3]
    dbPath = sys.argv[4]

    # Delete DB before running the program
    open(dbPath, 'w').close() 
    open(outputPath, 'w').close() 

    # Create DB.
    repo = _Reposetory(dbPath)
    atexit.register(repo._close)
    repo.createTables() # it doesn't recreate them
    
    # Build pizzeria according to configuration.
    repo.buildPizzeria(configPath)

    # Read config file.
    orderId = 1
    with open(ordersPath) as file:

        # Read the entire file.
        for line in file:

            # Get order.
            (location, topping) = line.rstrip('\n').split(',')

            # Check for possible suppliers.
            possible_suppliers = repo.getSuppliers(topping)

            # if no suppliers, dont order
            if (len(possible_suppliers) == 0):
                continue

            # Choose supplier with min id.
            supplierId = min([supp.id for supp in possible_suppliers])

            # Order pizza.
            output = repo.orderPizza(Order(orderId, location, repo.hats.findByTopping(topping, supplierId).id))
            orderId += 1

            # Log. 
            with open(outputPath, 'a') as file:
                # print(','.join(list(output.__dict__.values())) + '\n')
                file.write(','.join(list(output.__dict__.values())) + '\n')


if __name__ == "__main__":
    main()



    # hats = repo.hats.findall()
    # for hat in hats:
    #     print(hat.id, hat.topping, hat.supplier, hat.quantity)

    # suppliers = repo.suppliers.findall()
    # for supplier in suppliers:
    #     print(supplier.id, supplier.name)
    # return