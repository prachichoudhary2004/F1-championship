-- F1 Race Intelligence Lakehouse - Business Analytics Queries
-- These queries are designed to be run on the Gold layer tables

-- 1. Championship Contenders
SELECT 
    forename, 
    surname, 
    year, 
    total_points, 
    championship_position,
    championship_battle_status
FROM gold.championship_standings
WHERE year = 2023
ORDER BY total_points DESC;

-- 2. Team Dominance Analysis
SELECT 
    constructorName, 
    year, 
    total_wins, 
    team_win_rate,
    dominance_score
FROM gold.constructor_rankings
ORDER BY dominance_score DESC
LIMIT 10;

-- 3. Driver Consistency vs Aggression
SELECT 
    forename, 
    surname, 
    consistency_score, 
    win_rate,
    form_trend
FROM gold.driver_consistency
JOIN gold.driver_statistics USING (driverId, year)
WHERE total_races > 5
ORDER BY consistency_score DESC;

-- 4. Pit Stop Efficiency by Team
SELECT 
    constructorName, 
    AVG(average_pit_time) as avg_pit_time,
    AVG(pit_consistency) as avg_consistency
FROM gold.pit_stop_efficiency
GROUP BY constructorName
ORDER BY avg_pit_time ASC;
