use College;
select * from Employees
INSERT INTO Employees (EmployeeID, firstName, lastName,
Position, Department, HireDate)
VALUES (1, 'Amit', 'Sharma', 'Software Engineer', 'IT','2022-01-15'),
(2, 'Priya', 'Mehta', 'Project Manager', 'Operations','2023-02-20'),
(3, 'Raj', 'Patel', 'Business Analyst', 'Finance','2021-06-30'),
(4, 'Sunita', 'Verma', 'HR Specialist', 'HR','2019-08-12'),
(5, 'Vikram', 'Rao', 'Software Engineer', 'IT','2021-03-18'),
(6, 'Anjali', 'Nair', 'HR Manager', 'HR','2020-05-14'),
(7, 'Rohan', 'Desai', 'Finance Manager', 'Finance','2022-11-25'),
(8, 'Sneha', 'Kumar', 'Operations Coordinator', 'Operations','2023-07-02'),
(9, 'Deepak', 'Singh', 'Data Scientist', 'IT','2022-08-05'),
(10, 'Neha', 'Gupta', 'Business Analyst', 'Finance','2020-10-10')
SELECT firstName, lastName, department from Employees;
select * from Employees where Department ='IT';
select * from Employees where HireDate >'2022-01-01';
SELECT * FROM Employees WHERE Department IN ('IT', 'HR');
SELECT * FROM Employees WHERE Department ='IT' AND HireDate>'2022-01-01';
SELECT DISTINCT Department from Employees;
select * from Employees where Department= 'IT' or HireDate>'2022-01-01';
select * from Employees where HireDate between '2022-01-01' and '2022-12-31';
select * from Employees where lastName like 's%';
select firstName + ' '+ lastName AS fullName, department from Employees;
select E.firstName , E.lastName, E.Department from Employees as E where E.Department ='IT';
select count(*) as employeecount from Employees;
select department , count(*) as employeecount from Employees group by Department;
create table Departments (departmentID int primary key, departmentName varchar(50));
insert into Departments (departmentID, departmentName)
values(1, 'IT'),
(2, 'HR'),
(3, 'Finance'), 
(4, 'Operations')
select e.employeeID , e.firstName, e.lastName , d.DepartmentName from Employees e Join Departments
d on e.Department = d.departmentName;
select firstName , lastName from Employees where HireDate =(select min(Hiredate) from Employees);
select firstName from Employees where Department in(select Department from Employees group by Department having count(Department)>2);