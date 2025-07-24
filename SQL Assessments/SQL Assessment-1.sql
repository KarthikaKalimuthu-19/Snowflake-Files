-- SQL Assignment – Personal Fitness Tracker

CREATE DATABASE Personal_Fitness_Tracker;
USE Personal_Fitness_Tracker;

-- Table 1: Exercises

CREATE TABLE Exercises (
  exercise_id INT PRIMARY KEY,
  exercise_name VARCHAR(50),
  category VARCHAR(20),
  calories_burn_per_min DECIMAL(5,2)
);

INSERT INTO Exercises VALUES
(1, 'Running', 'Cardio', 10.0),
(2, 'Cycling', 'Cardio', 8.0),
(3, 'Weight Lifting', 'Strength', 6.5),
(4, 'Yoga', 'Flexibility', 4.0),
(5, 'Jump Rope', 'Cardio', 11.0);

-- Table 2: WorkoutLog

CREATE TABLE WorkoutLog (
  log_id INT PRIMARY KEY,
  exercise_id INT,
  date DATE,
  duration_min INT,
  mood VARCHAR(20),
  FOREIGN KEY (exercise_id) REFERENCES Exercises(exercise_id)
);

INSERT INTO WorkoutLog VALUES
(101, 1, '2025-03-02', 30, 'Energized'),
(102, 1, '2025-03-05', 45, 'Tired'),
(103, 2, '2025-02-25', 40, 'Normal'),
(104, 2, '2025-03-10', 35, 'Energized'),
(105, 3, '2025-01-15', 60, 'Tired'),
(106, 3, '2025-03-12', 50, 'Normal'),
(107, 4, '2025-03-03', 45, 'Energized'),
(108, 4, '2025-02-20', 30, 'Normal'),
(109, 5, '2025-03-15', 25, 'Tired'),
(110, 5, '2025-03-20', 35, 'Normal');

-- Queries to Practice

-- Basic Queries
-- 1. Show all exercises under the “Cardio” category.
SELECT * FROM Exercises
WHERE category = "Cardio";

-- 2. Show workouts done in the month of March 2025.
SELECT * FROM WorkoutLog
WHERE MONTH(date) = 03 AND YEAR(date) = 2025;

-- Calculations
-- 3. Calculate total calories burned per workout (duration × calories_burn_per_min).
SELECT w.log_id, e.exercise_id, w.duration_min, w.duration_min * e.calories_burn_per_min AS tot_calories FROM WorkoutLog AS w
JOIN Exercises AS e 
ON w.exercise_id = e.exercise_id;

-- 4. Calculate average workout duration per category.
SELECT e.category, AVG(w.duration_min) AS avg_duration FROM WorkoutLog w
JOIN Exercises AS e 
ON w.exercise_id = e.exercise_id
GROUP BY e.category;

-- JOIN + Aggregation
-- 5. List exercise name, date, duration, and calories burned using a join.
SELECT e.exercise_name, w.date, w.duration_min, w.duration_min * e.calories_burn_per_min AS calories_burned FROM WorkoutLog AS w
JOIN Exercises AS e 
ON w.exercise_id = e.exercise_id;

-- 6. Show total calories burned per day.
SELECT w.date, SUM(w.duration_min * e.calories_burn_per_min) AS tot_calories FROM WorkoutLog AS w
JOIN Exercises AS e 
ON w.exercise_id = e.exercise_id
GROUP BY w.date;

-- Subqueries
-- 7. Find the exercise that burned the most calories in total.
SELECT e.exercise_name, SUM(w.duration_min * e.calories_burn_per_min) AS tot_calories FROM WorkoutLog AS w
JOIN Exercises AS e 
ON w.exercise_id = e.exercise_id
GROUP BY e.exercise_name
ORDER BY tot_calories DESC
LIMIT 1;

-- 8. List exercises never logged in the workout log.
SELECT * FROM Exercises
WHERE exercise_id NOT IN (SELECT DISTINCT exercise_id FROM WorkoutLog);

-- Conditional + Text Filters
-- 9. Show workouts where mood was “Tired” and duration > 30 mins.
SELECT * FROM WorkoutLog
WHERE mood = 'Tired' AND duration_min > 30;

-- 10. Update a workout log to correct a wrongly entered mood.
UPDATE WorkoutLog SET mood = 'Normal' WHERE log_id = 102;

-- Update & Delete
-- 11. Update the calories per minute for “Running”.
UPDATE Exercises SET calories_burn_per_min = 11.5 WHERE exercise_name = 'Running';

-- 12. Delete all logs from February 2024.
DELETE FROM WorkoutLog WHERE MONTH(date) = 2 AND YEAR(date) = 2024;