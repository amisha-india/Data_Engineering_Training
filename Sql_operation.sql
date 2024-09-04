CREATE TABLE Customers 
( CustomerID INT PRIMARY KEY ,
FirstName VARCHAR(50),
LastName VARCHAR (50),
City varchar(100),
Email nvarchar(100));
INSERT INTO Customers (CustomerID, FirstName, LastName, City, Email)
VALUES 
(1,'Amit', 'Sharma', 'Mumbai', 'amit.sharma@example.com'),
(2,'Priya', 'Mehta', 'Delhi', 'priya.mehta@example.com'),
(3,'Rohit', 'Kumar', 'Bangalore', 'rohit.kumar@example.com'),
(4,'Neha', 'Verma', 'Mumbai', 'neha.verma@example.com'),
(5,'Siddharth', 'Singh', 'Chennai', 'siddharth.singh@example.com'),
(6,'Asha', 'Rao', 'Hyderabad', 'asha.rao@example.com');
CREATE TABLE Orders (
OrderID INT PRIMARY KEY ,
CustomerID INT,
OrderAmount DECIMAL (10,2),
OrderDate DATE,
FOREIGN KEY (CustomerID) REFERENCES Customers (CustomerID));
INSERT INTO Orders (OrderID, CustomerID, OrderAmount, OrderDate)
VALUES 
(1,1, 1500.00, '2024-08-01'),
(2,2, 800.00, '2024-08-02'),
(3,3, 3000.00, '2024-08-03'),
(4,4, 1200.00, '2024-08-04'),
(5,5, 600.00, '2024-08-05'),
(6,6, 2200.00, '2024-08-06'),
(7,1, 2000.00, '2024-08-07'),
(8,2, 300.00, '2024-08-08'),
(9,3, 1000.00, '2024-08-09'),
(10,4, 1800.00, '2024-08-10'),
(11,5, 400.00, '2024-08-11'),
(12,6, 1000.00, '2024-08-12');

--1. Filter and Aggregate on Join Results using SQL**
-- Task: Join the `Orders` and `Customers` tables to find the total order amount per customer and filter out customers who have spent less than $1,000.

SELECT c.CustomerID, c.FirstName, c.LastName, SUM(o.OrderAmount) AS TotalSpent
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
HAVING SUM(o.OrderAmount) >= 1000;

-- 2. Cumulative Aggregations and Ranking in SQL Queries
-- Create a cumulative sum of the OrderAmount for each customer 
-- to track the running total of how much each customer has spent.

SELECT c.CustomerID, c.FirstName, c.LastName, o.OrderAmount, 
SUM(o.OrderAmount) OVER (PARTITION BY c.CustomerID ORDER BY o.OrderDate) AS RunningTotal
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID;
ORDER by c.CustomerID, o.OrderDate;


-- 3. OVER and PARTITION BY Clause in SQL Queries
-- Rank the customers based on the total amount they have spent, partitioned by city.

SELECT c.CustomerID, c.FirstName, c.LastName, SUM(o.OrderAmount) AS TotalSpent,
RANK() OVER (PARTITION BY c.City ORDER BY SUM(o.OrderAmount) DESC) AS RankByCity
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName, c.City;


-- 4. Total Aggregation using OVER and PARTITION BY in SQL Queries
-- Calculate the total amount of all orders (overall total) 
-- and the percentage each customer's total spending contributes to the overall total.

WITH TotalSpending AS (
    SELECT c.CustomerID, c.FirstName, c.LastName, SUM(o.OrderAmount) AS TotalSpent
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    GROUP BY c.CustomerID, c.FirstName, c.LastName
)
SELECT CustomerID, FirstName, LastName, TotalSpent, 
       (TotalSpent * 100.0 / SUM(TotalSpent) OVER ()) AS PercentageOfTotal
FROM TotalSpending;


-- 5. Ranking in SQL
-- Rank all customers based on the total amount they have spent, without partitioning.

SELECT c.CustomerID, c.FirstName, c.LastName, SUM(o.OrderAmount) AS TotalSpent,
RANK() OVER (ORDER BY SUM(o.OrderAmount) DESC) AS OverallRank
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName;


-- 6. Calculate the Average Order Amount per City
-- Write a query that joins the Orders and Customers tables,
-- calculates the average order amount for each city, 
-- and orders the results by the average amount in descending order.

SELECT c.City, AVG(o.OrderAmount) AS AvgOrderAmount
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.City
ORDER BY AvgOrderAmount DESC;


-- 7.  Find Top N Customers by Total Spending
-- Write a query to find the top 3 customers who have spent the most, using ORDER BY and LIMIT.

SELECT TOP 3 c.CustomerID, c.FirstName, c.LastName, SUM(o.OrderAmount) AS TotalSpent
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY TotalSpent DESC;


-- 8. Calculate Yearly Order Totals
-- Write a query that groups orders by year (using OrderDate),
--calculates the total amount of orders for each year, and orders the results by year.

SELECT YEAR(o.OrderDate) AS OrderYear, SUM(o.OrderAmount) AS YearlyTotal
FROM Orders o
GROUP BY YEAR(o.OrderDate)
ORDER BY OrderYear;


-- 9. Calculate the Rank of Customers by Total Order Amount in a Specific City
-- Write a query that ranks customers by their total spending,
-- but only for customers located in "Mumbai". The rank should reset for each customer in "Mumbai"

SELECT c.CustomerID, c.FirstName, c.LastName, SUM(o.OrderAmount) AS TotalSpent,
RANK() OVER (PARTITION BY c.City ORDER BY SUM(o.OrderAmount) DESC) AS RankInMumbai
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE c.City = 'Mumbai'
GROUP BY c.CustomerID, c.FirstName, c.LastName, c.City;


-- 10. Compare Each Customer's Total Order to the Average Order Amount
--  Write a query that calculates each customer's total order amount
-- and compares it to the average order amount for all customers.

WITH Totalspend AS (
	SELECT c.CustomerID, c.FirstName, c.LastName, 
	SUM(o.Orderamount) AS totalspent
	FROM customers c
	JOIN orders o ON c.CustomerID = o.CustomerID
	GROUP BY c.CustomerID, c.FirstName, c.LastName
)
SELECT CustomerID, FirstName, LastName, totalspent,
AVG(totalspent) OVER () AS avgtotalspent,
totalspent - AVG(totalspent) OVER () AS Differavg
from 
Totalspend;

