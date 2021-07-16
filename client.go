package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
)

var commands = []string{"DEPOSIT", "WITHDRAW", "BALANCE", "COMMIT", "ABORT"}

var commandHandled = make(chan bool, 1)

type Host struct {
	address string
	port    string
}

type Client struct {
	ID              string
	branches        map[string]Host
	numTransactions int
	transactionID   string
	coordinator     net.Conn
	inTransaction   bool
}

/*
Sends request to coordinator server with command and transaction ID
*/
func requestCoordinator(client *Client, command string) {
	fmt.Fprintf(client.coordinator, command+" "+client.transactionID+"\n")
	response, _ := bufio.NewReader(client.coordinator).ReadString('\n')
	// Abort
	if strings.Contains(response, "ABORTED") || strings.Contains(response, "COMMIT") {
		client.inTransaction = false
	}
	fmt.Print(response)
}

/*
Open a new transaction.
The client must connect to a randomly selected server which will coordinate
the transaction, and reply “OK” to the user.
*/
func beginHandler(client *Client) {
	// Increment number of transactions for new transaction ID
	client.numTransactions++
	client.transactionID = client.ID + "." + strconv.Itoa(client.numTransactions)
	// TODO: change to randomly chosen coordinator
	// coordBranch := "A"
	branchNames := []string{"A", "B", "C", "D", "E"}
	randSeed := rand.Intn(5)
	coordBranch := branchNames[randSeed]
	coordServer := client.branches[coordBranch]
	c, err := net.Dial("tcp", coordServer.address+":"+coordServer.port)
	if err != nil {
		log.Fatal(err)
	}
	client.coordinator = c
	requestCoordinator(client, "BEGIN")
}

func valid(command string) bool {
	for _, v := range commands {
		if v == command {
			return true
		}
	}
	return false
}

func handleCommand(command string, client *Client) {
	args := strings.Split(command, " ")
	currCommand := args[0]

	// fmt.Println("curr command: ", currCommand)

	if client.inTransaction {
		if valid(currCommand) {
			requestCoordinator(client, command)
		} else {
			// fmt.Println("Invalid command " + currCommand)
		}
	} else {
		if currCommand == "BEGIN" {
			client.inTransaction = true
			beginHandler(client)
		} else {
			// fmt.Println("Invalid command " + currCommand)
		}
	}
}

func main() {
	args := os.Args
	if len(args) != 3 {
		log.Fatal("Please provide client ID and config file")
	}
	id := args[1]

	// fmt.Println("args are ", args)

	// Create client
	client := new(Client)
	client.ID = id
	branches := make(map[string]Host)
	client.branches = branches
	client.numTransactions = 0
	client.inTransaction = false

	// Read config file
	configFile := args[2]
	file, err := os.Open(configFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := strings.Split(scanner.Text(), " ")
		client.branches[text[0]] = Host{text[1], text[2]}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// fmt.Println("Client branches: ", client.branches)

	// Read from standard input
	input := bufio.NewReader(os.Stdin)
	for {
		command, _, err := input.ReadLine()
		if err != nil {
			log.Fatal(err)
		} else {
			// fmt.Println("command: ", string(command))
			handleCommand(string(command), client)
		}
		// <-commandHandled
	}
}
