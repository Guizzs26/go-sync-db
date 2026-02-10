package main

type Order struct {
	ID        string  `json:"id"`
	Customer  string  `json:"customer"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	Timestamp string  `json:"timestamp"`
}
