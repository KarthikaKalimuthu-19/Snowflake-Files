-- SQL Assignment – Pet Clinic Management

CREATE DATABASE Pet_Clinic_Management;
USE Pet_Clinic_Management;

-- Table 1: Pets

CREATE TABLE Pets (
pet_id INT PRIMARY KEY,
name VARCHAR(50),
type VARCHAR(20),
breed VARCHAR(50),
age INT,
owner_name VARCHAR(50)
);

INSERT INTO Pets VALUES
(1, 'Buddy', 'Dog', 'Golden Retriever', 5, 'Ayesha'),
(2, 'Mittens', 'Cat', 'Persian', 3, 'Rahul'),
(3, 'Rocky', 'Dog', 'Bulldog', 6, 'Sneha'),
(4, 'Whiskers', 'Cat', 'Siamese', 2, 'John'),
(5, 'Coco', 'Parrot', 'Macaw', 4, 'Divya'),
(6, 'Shadow', 'Dog', 'Labrador', 8, 'Karan');

-- Table 2: Visits

CREATE TABLE Visits (
visit_id INT PRIMARY KEY,
pet_id INT,
visit_date DATE,
issue VARCHAR(100),
fee DECIMAL(8,2),
FOREIGN KEY (pet_id) REFERENCES Pets(pet_id)
);

INSERT INTO Visits VALUES
(101, 1, '2024-01-15', 'Regular Checkup', 500.00),
(102, 2, '2024-02-10', 'Fever', 750.00),
(103, 3, '2024-03-01', 'Vaccination', 1200.00),
(104, 4, '2024-03-10', 'Injury', 1800.00),
(105, 5, '2024-04-05', 'Beak trimming', 300.00),
(106, 6, '2024-05-20', 'Dental Cleaning', 950.00),
(107, 1, '2024-06-10', 'Ear Infection', 600.00);

-- Query Tasks
-- Basics

-- 1. List all pets who are dogs.
SELECT * FROM Pets 
WHERE type = "Dog";

-- 2. Show all visit records with a fee above 800.
SELECT * FROM Visits
where fee > 800;

-- Joins
-- 3. List pet name, type, and their visit issues.
SELECT p.name, p.type, v.issue FROM Pets AS p
JOIN Visits AS v
ON p.pet_id = v.pet_id;

-- 4. Show the total number of visits per pet.
SELECT p.name, COUNT(v.visit_id) no_of_visits FROM Pets as p
JOIN Visits AS v
ON p.pet_id = v.pet_id
GROUP BY v.pet_id;

-- Aggregation
-- 5. Find the total revenue collected from all visits.
SELECT SUM(fee) Tot_revenue FROM Visits;

-- 6. Show the average age of pets by type.
SELECT type, AVG(age) Avg_age FROM Pets
GROUP BY type;

-- Date & Filtering
-- 7. List all visits made in the month of March.
SELECT * FROM Visits
WHERE MONTH(visit_date) = 03;

-- 8. Show pet names who visited more than once.
SELECT p.name Pet_name, COUNT(v.visit_id) Most_visited FROM Pets AS p
JOIN Visits AS v
ON p.pet_id = v.pet_id
GROUP BY v.pet_id
HAVING Most_visited > 1;

-- Subqueries
-- 9. Show the pet(s) who had the costliest visit.
SELECT p.name AS Costly_pet, v.fee FROM Pets AS p
JOIN Visits AS v
ON p.pet_id = v.pet_id
WHERE v.fee = (SELECT MAX(fee) AS Highest_fee FROM Visits);

-- 10. List pets who haven’t visited the clinic yet.
SELECT * FROM Pets
WHERE pet_id NOT IN (SELECT DISTINCT pet_id FROM Visits);

-- Update & Delete
-- 11. Update the fee for visit_id 105 to 350.
UPDATE Visits SET fee = 350 WHERE visit_id = 105;

-- 12. Delete all visits made before Feb 2024.
DELETE FROM Visits WHERE visit_date < '2024-02-01';