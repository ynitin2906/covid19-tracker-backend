package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

type TimeSeriesData struct {
	Date                time.Time `json:"date"`
	LocationKey         string    `json:"location_key"`
	NewConfirmed        int32     `json:"new_confirmed"`
	NewDeceased         int32     `json:"new_deceased"`
	NewRecovered        int32     `json:"new_recovered"`
	NewTested           int32     `json:"new_tested"`
	CumulativeConfirmed int32     `json:"cumulative_confirmed"`
	CumulativeDeceased  int32     `json:"cumulative_deceased"`
	CumulativeRecovered int32     `json:"cumulative_recovered"`
	CumulativeTested    int32     `json:"cumulative_tested"`
}

type FilterRequest struct {
	LocationKey string `json:"location_key"` // Optional: key for filtering by location
	StartDate   string `json:"start_date"`   // Optional: start date for filtering
	EndDate     string `json:"end_date"`     // Optional: end date for filtering
}

var db clickhouse.Conn

func main() {
	var err error
	// Connect to ClickHouse database
	db, err = connectClickhouse()
	if err != nil {
		log.Fatalf("failed to connect to ClickHouse: %v", err)
	}

	app := fiber.New()

	app.Use(cors.New(cors.Config{
		AllowOrigins: "http://localhost:3000", // Update this to allow specific frontend origin
		AllowMethods: "GET,POST,HEAD,PUT,DELETE,PATCH",
	}))

	app.Post("/api/timeseries", getTimeSeries)

	log.Fatal(app.Listen(":8080"))
}

// connectClickhouse establishes a connection to the ClickHouse database
func connectClickhouse() (clickhouse.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"}, // Use the appropriate ClickHouse address
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout: 5 * time.Second,
	})
}

func getTimeSeries(c *fiber.Ctx) error {
	var filter FilterRequest
	if err := c.BodyParser(&filter); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid filter parameters"})
	}

	// Start building the query
	query := `
	WITH latest_deaths_data AS (
		SELECT location_key,
			   date,
			   new_deceased,
			   new_confirmed,
			   new_recovered,
			   new_tested,
			   cumulative_confirmed,
			   cumulative_deceased,
			   cumulative_recovered,
			   cumulative_tested,
			   ROW_NUMBER() OVER (PARTITION BY location_key ORDER BY date DESC) AS rn
		FROM covid19
	)
	SELECT location_key,
		   date,
		   new_deceased,
		   new_confirmed,
		   new_recovered,
		   new_tested,
		   cumulative_confirmed,
		   cumulative_deceased,
		   cumulative_recovered,
		   cumulative_tested
	FROM latest_deaths_data
	WHERE rn = 1
	`

	var args []interface{}
	conditions := []string{}

	// Adding filters for date range and location_key if provided
	if filter.StartDate != "" && filter.EndDate != "" {
		conditions = append(conditions, "date BETWEEN ? AND ?")
		args = append(args, filter.StartDate, filter.EndDate)
	}

	if filter.LocationKey != "" {
		conditions = append(conditions, "location_key = ?")
		args = append(args, filter.LocationKey)
	}

	// Join the conditions with " AND " and add to the query if there are any
	if len(conditions) > 0 {
		query += " AND " + joinConditions(conditions, " AND ")
	}

	// Execute the query
	rows, err := db.Query(context.Background(), query, args...)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Query execution failed: " + err.Error()})
	}
	defer rows.Close()

	var data []TimeSeriesData
	for rows.Next() {
		var ts TimeSeriesData
		if err := rows.Scan(
			&ts.LocationKey,
			&ts.Date,
			&ts.NewConfirmed,
			&ts.NewDeceased,
			&ts.NewRecovered,
			&ts.NewTested,
			&ts.CumulativeConfirmed,
			&ts.CumulativeDeceased,
			&ts.CumulativeRecovered,
			&ts.CumulativeTested,
		); err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Row scan failed: " + err.Error()})
		}
		data = append(data, ts)
	}

	if err := rows.Err(); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Error reading rows"})
	}

	return c.JSON(data)
}

// joinConditions joins the slice of conditions with the specified separator
func joinConditions(conditions []string, separator string) string {
	return fmt.Sprintf("(%s)", join(conditions, separator))
}

// join joins a slice of strings with a separator
func join(strings []string, separator string) string {
	if len(strings) == 0 {
		return ""
	}
	result := strings[0]
	for _, str := range strings[1:] {
		result += separator + str
	}
	return result
}
