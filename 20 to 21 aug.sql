create database Company;
use company;
create table tbleEmployee (Id int not null primary key , 
Name varchar(50), Gender varchar(50), Salary int , 
DepartmentId int );
create table tblDepartment (Id int not null primary key , DepartmentName varchar(50), Location varchar(50), 
DepartmentHead varchar(50))
Insert into tbleEmployee (Id , Name , Gender , Salary , DepartmentId)
values (1, 'Tom', 'Male', 4000, 1),
(2, 'Pam', 'Female', 3000, 3),
(3, 'John', 'Male', 3500, 1),
(4, 'Sam', 'Male', 4500, 2),
(5, 'Todd', 'Male', 2800, 2),
(6, 'Ben', 'Male', 7000, 1),
(7, 'Sara', 'Female', 4800, 3),
(8, 'Valarie', 'Female', 5500, 1),
(9, 'James', 'Male', 6500, NULL),
(10, 'Russell', 'Male', 8800, NULL)
Insert into tblDepartment(Id, DepartmentName, Location, DepartmentHead)
values (1, 'IT', 'London', 'Rick'),
(2, 'Payroll', 'Delhi', 'Ron'),
(3, 'HR', 'New york', 'Christie'),
(4, 'Other department', 'Sydney', 'Cindrella')

--Inner joining
select Name, Gender , Salary , DepartmentName from  tbleEmployee e
INNER JOIN tblDepartment d on e.DepartmentId = d.Id
--Left join
select Name, Gender , salary , DepartmentName from tbleEmployee e left join tblDepartment
d on e. DepartmentId = d.Id;
--Right join
select Name, Gender , salary , DepartmentName from tbleEmployee e Right join tblDepartment
d on e. DepartmentId = d.Id;
--full join 
select Name, Gender , salary , DepartmentName from tbleEmployee e full outer  join tblDepartment
d on e. DepartmentId = d.Id;

--practice
--Create table products and orders
Create table Products (Product_id int not null primary key , Product_name varchar(50),
price int);
Create table Orders(Order_id int not null primary key , product_id int ,
Quantity int , order_date Date);

--Insert values
INSERT INTO Products(Product_id, Product_name, price)
VALUES (1, 'Laptop', 800),
(2, 'Smartphone', 500),
(3, 'Tablet', 300),
(4, 'Headphones', 50),
(5, 'Monitor', 150);
INSERT INTO Orders (Order_id, product_id, Quantity, order_date)
VALUES (1, 1, 2, '2024-08-01'),
(2, 2, 1, '2024-08-02'),
(3, 3, 3, '2024-08-03'),
(4, 1, 1, '2024-08-04'),
(5, 4, 4, '2024-08-05'),
(6, 5, 2, '2024-08-06'),
(7, 6, 1, '2024-08-07');

--INNER JOIN
SELECT product_name , order_date, Quantity from Products INNER JOIN 
Orders on Products.Product_id =Orders.product_id
--Left outer join
SELECT product_name , order_date, Quantity from Products LEFT OUTER JOIN 
Orders on Products.Product_id =Orders.product_id
--Right outer join
SELECT product_name , order_date, Quantity from Products RIGHT OUTER JOIN 
Orders on Products.Product_id =Orders.product_id
--full outer join
SELECT product_name , order_date, Quantity from Products FULL OUTER JOIN 
Orders on Products.Product_id =Orders.product_id;
select * from Products;
select * from orders;

--grouping sets
select p.product_name, o.order_date , sum(o.quantity) AS total_quantity
GROUP BY GROUPING SETS ((p.product_name), (o.order_id));

--sub query
select o.order_id , o.product_id (select p.product_name from Products p where
p.Product_id = o.product_id) as Product_name from Orders o;

Select order_id , order_date , product_id from orders where product_id IN
(select product_id from products where price>500);
select p.product_name , p.price from products p where p.price > ALL (select price from products where product_name LIKE 'Smartphone%');
select order_id  from orders where product_id IN (select Product_id from Products where price > 500)
select product_name from Products where price>500 
union
select product_name from products where product_name LIKE 'Smart%';
select product_name from Products where price>500 
EXCEPT
select product_name from products where product_name LIKE 'Smart%';


CREATE TABLE Employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(255),
    department VARCHAR(255),
    manager_id INT
);

CREATE TABLE Salaries (
    salary_id INT PRIMARY KEY,
    employee_id INT,
    salary DECIMAL(10, 2),
    salary_date DATE,
    FOREIGN KEY (employee_id) REFERENCES Employees(employee_id)
);



INSERT INTO Employees (employee_id, employee_name, department, manager_id) VALUES
(1, 'John Doe', 'HR', NULL),
(2, 'Jane Smith', 'Finance', 1),
(3, 'Robert Brown', 'Finance', 1),
(4, 'Emily Davis', 'Engineering', 2),
(5, 'Michael Johnson', 'Engineering', 2);

INSERT INTO Salaries (salary_id, employee_id, salary, salary_date) VALUES
(1, 1, 5000, '2024-01-01'),
(2, 2, 6000, '2024-01-15'),
(3, 3, 5500, '2024-02-01'),
(4, 4, 7000, '2024-02-15'),
(5, 5, 7500, '2024-03-01');

--using equi join
select e.employee_name , s.salary from Employees e  join Salaries s on e.employee_id=s.employee_id;
--using self join
SELECT e1.employee_name AS Employee, e2.employee_name AS Manager
FROM Employees e1
LEFT JOIN Employees e2 ON e1.manager_id = e2.employee_id;
--using group by with having 
SELECT e.department, AVG(s.salary) AS avg_salary
FROM Employees e
JOIN Salaries s ON e.employee_id = s.employee_id
GROUP BY e.department
HAVING AVG(s.salary) >= 6000;
--using group by with grouping sets
select e.department, sum(s.salary) as total_salary from Employees e join salaries s 
on e.employee_id = s.employee_id
group by grouping sets (e.department, ());
--using 

Select salary , power(salary,2) as pricepower from Salaries;
select salary , SQRT (salary) as slf from salaries;
