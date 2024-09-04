create database Onlineshoping;
use Onlineshoping;
--1. Table: `Customers`**

--Create Customers Table:

CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    PhoneNumber VARCHAR(15)
);


--Insert Sample Data into `Customers` (Indian Names):


INSERT INTO Customers (FirstName, LastName, Email, PhoneNumber)
VALUES 
('Amit', 'Sharma', 'amit.sharma@example.com', '9876543210'),
('Priya', 'Mehta', 'priya.mehta@example.com', '8765432109'),
('Rohit', 'Kumar', 'rohit.kumar@example.com', '7654321098'),
('Neha', 'Verma', 'neha.verma@example.com', '6543210987'),
('Siddharth', 'Singh', 'siddharth.singh@example.com', '5432109876'),
('Asha', 'Rao', 'asha.rao@example.com', '4321098765');


--2. Table: `Products`**

--Create `Products` Table:


CREATE TABLE Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(10, 2),
    StockQuantity INT
);


--Insert Sample Data into `Products`:

INSERT INTO Products (ProductName, Category, Price, StockQuantity)
VALUES 
('Laptop', 'Electronics', 75000.00, 15),
('Smartphone', 'Electronics', 25000.00, 30),
('Desk Chair', 'Furniture', 5000.00, 10),
('Monitor', 'Electronics', 12000.00, 20),
('Bookshelf', 'Furniture', 8000.00, 8);

-- **3. Table: `Orders`**

-- Create `Orders` Table:


CREATE TABLE Orders (
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    TotalAmount DECIMAL(10, 2),
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);


--Insert Sample Data into `Orders`:

INSERT INTO Orders (CustomerID, ProductID, Quantity, TotalAmount, OrderDate)
VALUES 
(1, 1, 2, 150000.00, '2024-08-01'),
(2, 2, 1, 25000.00, '2024-08-02'),
(3, 3, 4, 20000.00, '2024-08-03'),
(4, 4, 2, 24000.00, '2024-08-04'),
(5, 5, 5, 40000.00, '2024-08-05');

--Exercise-1
-- calculate the total amount spent by each customer
SELECT CustomerID, SUM(TotalAmount) AS total_spent
FROM Orders
GROUP BY CustomerID;

--find customers who have spent more than $ 1,000 in total.
SELECT CustomerID
FROM Orders
GROUP BY CustomerID
HAVING SUM(TotalAmount) > 1000;

--find product categories with more than 5 products
SELECT category_id
FROM Products
GROUP BY category_id
HAVING COUNT(*) > 5;

-- calculate the total number of products for each category and supplier combination
SELECT category_id, supplier_id, COUNT(*) AS total_products
FROM Products
GROUP BY category_id, supplier_id;

--Summarize total sales by product and customer, and also provide an overall total
SELECT ProductID, CustomerID, SUM(TotalAmount) AS total_sales
FROM Orders
GROUPING SETS ((ProductID), (CustomerID),());



--Exercise-2

--Stored procedure with Insert operation
CREATE PROCEDURE InsertEmployee

    @emp_id,
    @dept VARCHAR(50),
   
AS
BEGIN
    INSERT INTO Employees (employee_id, department) 
    VALUES (emp_id, dept);
END;
--Stored procedure with update operation

CREATE PROCEDURE UpdateEmployeeDepartment

    @emp_id INT,
    @new_dept VARCHAR(255)
AS
BEGIN
    UPDATE Employees
    SET department = @new_dept
    WHERE employee_id = @emp_id;
END;
--Stored procedure with delete operation

CREATE PROCEDURE DeleteEmployee
    @emp_id INT
AS
BEGIN
    DELETE FROM Employees
    WHERE employee_id = @emp_id;
END;

--Exercise 3
--1. Hands-on Exercise: Filtering Data using SQL Queries
--Retrieve all products from the Products table that belong to the category 'Electronics' and have a price greater than 500.

SELECT * FROM Products 
WHERE category = 'Electronics' AND price > 500;

--2. Hands-on Exercise: Total Aggregations using SQL Queries
--Calculate the total quantity of products sold from the Orders table.

SELECT SUM(quantity) AS Total_Quantity FROM Orders;

--3. Hands-on Exercise: Group By Aggregations using SQL Queries
--Calculate the total revenue generated for each product in the Orders table.

SELECT ProductId, SUM(TotalAmount) AS Total_Revenue FROM Orders GROUP BY ProductId;

--4. Hands-on Exercise: Order of Execution of SQL Queries
--Write a query that uses WHERE, GROUP BY, HAVING, and ORDER BY clauses and explain the order of execution.

SELECT ProductID, SUM(TotalAmount) AS Total_Revenue
FROM Orders
WHERE OrderDate < '2024-08-06'
GROUP BY ProductID
HAVING SUM(TotalAmount) > 500
ORDER BY Total_Revenue DESC;

--5. Hands-on Exercise: Rules and Restrictions to Group and Filter Data in SQL Queries
--Write a query that corrects a violation of using non-aggregated columns without grouping them.

SELECT ProductID, SUM(TotalAmount) AS Total_Revenue
FROM Orders
WHERE OrderDate >= '2024-08-01'
GROUP BY ProductID;

--6. Hands-on Exercise: Filter Data based on Aggregated Results using Group By and Having
--Retrieve all customers who have placed more than 5 orders using GROUP BY and HAVING clauses.

SELECT CustomerID 
FROM Orders 
GROUP BY CustomerID 
HAVING COUNT(OrderID) > 5;

--1. Basic Stored Procedure
--Create a stored procedure named GetAllCustomers that retrieves all customer details from the Customers table.

CREATE PROCEDURE GetAllCustomers
AS
BEGIN 
	SELECT * FROM Customers;
END;
EXEC GetAllCustomers;

--2. Stored Procedure with Input Parameter
--Create a stored procedure named GetOrderDetailsByOrderID that accepts an OrderID as a parameter and retrieves the order details for that specific order.

CREATE PROCEDURE GetOrderDetailsByOrderID 
	@OrderID INT
AS
BEGIN 
	SELECT * FROM Orders
	WHERE OrderID = @OrderID; 
END;
EXEC GetOrderDetailsByOrderID @OrderID=3 ;

--3. Stored Procedure with Multiple Input Parameters
--Create a stored procedure named GetProductsByCategoryAndPrice that accepts a product Category and a minimum Price as input parameters and retrieves all products that meet the criteria.

CREATE PROCEDURE GetProductsByCategoryAndPrice
	@Category VARCHAR(50), 
	@MinPrice DECIMAL(10, 2)
AS
BEGIN 
	SELECT * FROM Products 
	WHERE Category = @Category AND 
	Price >= @MinPrice; 
END;
EXEC  GetProductsByCategoryAndPrice @Category = 'Electronics',
@MinPrice =200.00;

--4. Stored Procedure with Insert Operation
--Create a stored procedure named InsertNewProduct that accepts parameters for ProductName, Category, Price, and StockQuantity and inserts a new product into the Products table.

CREATE PROCEDURE InsertNewProduct

    @ProductName VARCHAR (100),
	@Category VARCHAR(100),
    @Price INt,
	@StockQuantity INT
   
AS
BEGIN
    INSERT INTO Products(ProductName ,Category, Price, StockQuantity) 
    VALUES (ProductName ,Category, Price, StockQuantity);
END;

--5. Stored Procedure with Update Operation
--Create a stored procedure named UpdateCustomerEmail that accepts a CustomerID and a NewEmail parameter and updates the email address for the specified customer.

CREATE PROCEDURE UpdateCustomerEmail
	@CustomerID INT, 
	@NewEmail VARCHAR(100)
As
BEGIN 
	UPDATE Customers SET Email = @NewEmail 
	WHERE CustomerID = @CustomerID; 
END;

--6. Stored Procedure with Delete Operation
--Create a stored procedure named DeleteOrderByID that accepts an OrderID as a parameter and deletes the corresponding order from the Orders table.

CREATE PROCEDURE DeleteOrderByID
	@OrderID INT
AS
BEGIN 
	DELETE FROM Orders 
	WHERE OrderID = @OrderID; 
END;

--7. Stored Procedure with Output Parameter
--Create a stored procedure named GetTotalProductsInCategory that accepts a Category parameter and returns the total number of products in that category using an output parameter.

CREATE PROCEDURE GetTotalProductsInCategory
	@Category VARCHAR(50), 
	@TotalProducts INT OUTPUT
AS
BEGIN 
	SELECT @TotalProducts= COUNT(*) 
	FROM Products 
	WHERE Category = @Category; 
END;
DECLAR @TotalProducts INT;
EXEC GetTotalProductsInCategory @Category ='Electronics'
@TotalProducts = @Total OUTPUT;
SELECT @Total AS TotalProductsINCategory;





