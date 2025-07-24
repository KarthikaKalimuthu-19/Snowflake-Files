-- SQL Assignment – Travel Planner

CREATE DATABASE Travel_Planner;
USE Travel_Planner;

-- Table 1: Destinations

CREATE TABLE Destinations (
  destination_id INT PRIMARY KEY,
  city VARCHAR(50),
  country VARCHAR(50),
  category VARCHAR(20),
  avg_cost_per_day DECIMAL(10, 2)
);

INSERT INTO Destinations VALUES
(1, 'Goa', 'India', 'Beach', 2500),
(2, 'Agra', 'India', 'Historical', 2000),
(3, 'Paris', 'France', 'Historical', 4000),
(4, 'Bali', 'Indonesia', 'Beach', 2800),
(5, 'Zurich', 'Switzerland', 'Nature', 5000),
(6, 'Cape Town', 'South Africa', 'Adventure', 3500);

-- Table 2: Trips

CREATE TABLE Trips (
  trip_id INT PRIMARY KEY,
  destination_id INT,
  traveler_name VARCHAR(50),
  start_date DATE,
  end_date DATE,
  budget DECIMAL(10, 2),
  FOREIGN KEY (destination_id) REFERENCES Destinations(destination_id)
);

INSERT INTO Trips VALUES
(101, 1, 'Ayesha', '2023-12-01', '2023-12-07', 20000),
(102, 2, 'Rahul', '2023-11-10', '2023-11-12', 6000),
(103, 3, 'Sneha', '2024-01-05', '2024-01-15', 45000),
(104, 4, 'John', '2024-03-10', '2024-03-20', 32000),
(105, 5, 'Divya', '2022-12-15', '2022-12-25', 60000),
(106, 6, 'Karan', '2023-06-01', '2023-06-05', 18000),
(107, 2, 'Sneha', '2023-08-01', '2023-08-05', 8000),
(108, 1, 'Divya', '2023-09-15', '2023-09-18', 9000),
(109, 3, 'Ayesha', '2024-02-01', '2024-02-08', 35000),
(110, 4, 'Rahul', '2024-05-01', '2024-05-10', 30000);

-- Query Tasks

-- Basic Queries
-- 1. Show all trips to destinations in “India”.
SELECT t.* FROM Trips AS t
JOIN Destinations AS d 
ON t.destination_id = d.destination_id
WHERE d.country = 'India';

-- 2. List all destinations with an average cost below 3000.
SELECT * FROM Destinations
WHERE avg_cost_per_day < 3000;

-- Date & Duration
-- 3. Calculate the number of days for each trip.
SELECT trip_id, traveler_name, DATEDIFF(end_date, start_date) + 1 AS duration_days FROM Trips;

-- 4. List all trips that last more than 7 days.
SELECT trip_id, traveler_name, start_date, end_date FROM Trips
WHERE DATEDIFF(end_date, start_date) + 1 > 7;

-- JOIN + Aggregation
-- 5. List traveler name, destination city, and total trip cost (duration × avg_cost_per_day).
SELECT t.traveler_name, d.city, (DATEDIFF(t.end_date, t.start_date) + 1) * d.avg_cost_per_day AS tot_trip_cost FROM Trips AS t
JOIN Destinations AS d 
ON t.destination_id = d.destination_id;

-- 6. Find the total number of trips per country.
SELECT d.country, COUNT(*) AS trip_cnt FROM Trips AS t
JOIN Destinations AS d 
ON t.destination_id = d.destination_id
GROUP BY d.country;

-- Grouping & Filtering
-- 7. Show average budget per country.
SELECT d.country, AVG(t.budget) AS avg_budget FROM Trips AS t
JOIN Destinations AS d 
ON t.destination_id = d.destination_id
GROUP BY d.country;

-- 8. Find which traveler has taken the most trips.
SELECT traveler_name, COUNT(*) AS trip_count FROM Trips
GROUP BY traveler_name
ORDER BY trip_count DESC
LIMIT 1;

-- Subqueries
-- 9. Show destinations that haven’t been visited yet.
SELECT * FROM Destinations
WHERE destination_id NOT IN (SELECT DISTINCT destination_id FROM Trips);

-- 10. Find the trip with the highest cost per day.
SELECT trip_id, traveler_name, budget / (DATEDIFF(end_date, start_date) + 1) AS cost_per_day FROM Trips
ORDER BY cost_per_day DESC
LIMIT 1;

-- Update & Delete
-- 11. Update the budget for a trip that was extended by 3 days.
UPDATE Trips SET budget = 4000 * 14 WHERE trip_id = 103;

-- 12. Delete all trips that were completed before Jan 1, 2023.
DELETE FROM Trips WHERE end_date < '2023-01-01';
