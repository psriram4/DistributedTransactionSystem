package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Lock struct {
	mutex      *sync.RWMutex
	lockHolder string
	read       bool
	write      bool
}

type Server struct {
	// branch of current server
	branchID string
	// port of current server
	port string
	// map of branchIDs to address and port of servers
	branches map[string]string
	// stores account balances
	balances map[string]int
	// stores locks for accounts
	locks map[string]*Lock
	// for each transaction, stores accounts which it read from
	read map[string][]string
}

/*
When coordinator receives ABORT signal, it forwards to all servers
along with its transaction ID.

When server receives ABORT from coordinator, it goes through all locks and unlocks all locks
being held by that transaction ID.
*/
func yeetusFetus(transactionID string, server *Server) {
	// fmt.Println("COMMENCING YEETUS OF FETUS")
	for _, CONNECT := range server.branches {
		conn, err := net.Dial("tcp", CONNECT)
		if err != nil {
			fmt.Println("Could not connect to", CONNECT)
			log.Fatal(err)
		}
		fmt.Fprintf(conn, "ABORT "+transactionID+" \n")
		reply, _ := bufio.NewReader(conn).ReadString('\n')
		if reply != "ABORTED\n" {
			log.Fatal("ERROR ABORTING. REPLY: ", reply)
		}
	}
}

func coordinator(c net.Conn, server *Server) {
	// fmt.Println("HIT")
	// keep track of updates to accounts to commit once ready
	accountUpdates := make(map[string]int)
	// coordinator is spawned
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		// fmt.Println("Coordinator server " + server.branchID + " received message: " + netData)
		// extract information from message
		request := strings.Split(netData, " ")
		requestType := request[0]
		transactionID := request[len(request)-1]
		// fmt.Println("Transaction ID:", transactionID)
		switch requestType {
		case "BALANCE":
			accountID := request[1]
			accountInfo := strings.Split(accountID, ".")
			branch := accountInfo[0]
			// check if we can fetch balance from local updates
			if _, keyExists := accountUpdates[accountID]; keyExists {
				balance := strconv.Itoa(accountUpdates[accountID])
				response := accountID + " = " + balance
				// send response back to client
				c.Write([]byte(response + "\n"))
			} else {
				// send message to appropriate server
				CONNECT := server.branches[branch]
				serverConn, err := net.Dial("tcp", CONNECT)
				if err != nil {
					// fmt.Println("Could not connect to", CONNECT)
					log.Fatal(err)
				}
				fmt.Fprintf(serverConn, netData+" \n")

				// receive response from server - "BRANCH.ACCOUNT_NAME BALANCE" - "A.xyz 30\n"
				reply, _ := bufio.NewReader(serverConn).ReadString('\n')
				// CHECK FOR ABORTION
				if reply == "ABORTED\n" {
					go yeetusFetus(transactionID, server)
					c.Write([]byte("NOT FOUND, ABORTED\n"))
					return
				}
				responseInfo := strings.Split(reply, " ")
				balance, _ := strconv.Atoi(strings.TrimSpace(responseInfo[1]))
				accountUpdates[accountID] = balance
				balanceString := strconv.Itoa(accountUpdates[accountID])
				response := accountID + " = " + balanceString
				// send response back to client
				c.Write([]byte(response + "\n"))
			}

		case "DEPOSIT":
			accountID := request[1]
			accountInfo := strings.Split(accountID, ".")
			branch := accountInfo[0]
			deposit, _ := strconv.Atoi(strings.TrimSpace(request[2]))
			CONNECT := server.branches[branch]
			serverConn, err := net.Dial("tcp", CONNECT)
			if err != nil {
				fmt.Println("Could not connect to", CONNECT)
				log.Fatal(err)
			}
			// check if we can fetch balance from local updates
			if _, keyExists := accountUpdates[accountID]; keyExists {
				// Acquire write lock
				fmt.Fprintf(serverConn, "LOCK "+accountID+" "+transactionID+" \n")
				// fmt.Println("Waiting on Write Lock for deposit")
				reply, _ := bufio.NewReader(serverConn).ReadString('\n')
				if reply == "UNLOCKED\n" {
					accountUpdates[accountID] += deposit
					// balance := strconv.Itoa(accountUpdates[accountID])
					// response := accountID + " = " + balance
					// // send response back to client
					// c.Write([]byte(response + "\n"))
					c.Write([]byte("OK\n"))
				}
			} else {
				// send message to appropriate server
				fmt.Fprintf(serverConn, netData+" \n")
				// fmt.Println("Waiting for response for DEPOSIT from server")
				// receive response from server - "BRANCH.ACCOUNT_NAME BALANCE" - "A.xyz 30\n"
				reply, _ := bufio.NewReader(serverConn).ReadString('\n')
				responseInfo := strings.Split(reply, " ")
				curr, _ := strconv.Atoi(strings.TrimSpace(responseInfo[1]))
				// fmt.Println("reply is", responseInfo[1])
				// fmt.Println("curr is ", curr, " and deposit is ", deposit)
				accountUpdates[accountID] = curr + deposit
				// balance := strconv.Itoa(accountUpdates[accountID])
				// response := accountID + " = " + balance
				// // send response back to client
				// c.Write([]byte(response + "\n"))
				c.Write([]byte("OK\n"))
			}

		case "WITHDRAW":
			accountID := request[1]
			accountInfo := strings.Split(accountID, ".")
			branch := accountInfo[0]
			withdraw, _ := strconv.Atoi(request[2])
			// send message to appropriate server
			CONNECT := server.branches[branch]
			serverConn, err := net.Dial("tcp", CONNECT)
			if err != nil {
				fmt.Println("Could not connect to", CONNECT)
				log.Fatal(err)
			}
			// check if we can fetch balance from local updates
			if _, keyExists := accountUpdates[accountID]; keyExists {
				// Acquire write lock
				fmt.Fprintf(serverConn, "LOCK "+accountID+" "+transactionID+" \n")
				// fmt.Println("Waiting on Write Lock for Withdraw")
				reply, _ := bufio.NewReader(serverConn).ReadString('\n')
				if reply == "UNLOCKED\n" {
					accountUpdates[accountID] -= withdraw
					// balance := strconv.Itoa(accountUpdates[accountID])
					// response := accountID + " = " + balance
					// // send response back to client
					// c.Write([]byte(response + "\n"))
					c.Write([]byte("OK\n"))
				}
			} else {
				fmt.Fprintf(serverConn, netData+" \n")
				// fmt.Println("Waiting for response for WITHDRAW from server")
				// receive response from server - "BRANCH.ACCOUNT_NAME BALANCE" - "A.xyz 30\n"
				reply, _ := bufio.NewReader(serverConn).ReadString('\n')
				responseInfo := strings.Split(reply, " ")
				// CHECK FOR ABORTION
				if reply == "ABORTED\n" {
					go yeetusFetus(transactionID, server)
					c.Write([]byte("NOT FOUND, ABORTED\n"))
					return
				}
				curr, _ := strconv.Atoi(strings.TrimSpace(responseInfo[1]))
				accountUpdates[accountID] = curr - withdraw
				// balance := strconv.Itoa(accountUpdates[accountID])
				// response := accountID + " = " + balance
				// // send response back to client
				// c.Write([]byte(response + "\n"))
				c.Write([]byte("OK\n"))
			}

		case "COMMIT":
			// check if any updated balances have negative values
			for _, balance := range accountUpdates {
				if balance < 0 {
					// fmt.Println("Aborting - Negative balance found", account, balance)
					go yeetusFetus(transactionID, server)
					// send response back to client
					c.Write([]byte("ABORTED\n"))
					return
				}
			}
			// fmt.Println(accountUpdates)
			// if no consistency requirements violated, commit update
			for account, balance := range accountUpdates {
				accountInfo := strings.Split(account, ".")
				branch := accountInfo[0]
				CONNECT := server.branches[branch]
				serverConn, err := net.Dial("tcp", CONNECT)
				if err != nil {
					fmt.Println("Could not connect to", CONNECT)
					log.Fatal(err)
				}
				// send message to appropriate server
				fmt.Fprintf(serverConn, "COMMIT"+" "+account+" "+strconv.Itoa(balance)+" "+transactionID+" \n")
				// Get committed confirmation
				reply, _ := bufio.NewReader(serverConn).ReadString('\n')
				if reply != "COMMITTED\n" {
					go yeetusFetus(transactionID, server)
					c.Write([]byte("ABORTED\n"))
					return
				}
			}
			// Unlock read locks
			for _, CONNECT := range server.branches {
				serverConn, err := net.Dial("tcp", CONNECT)
				if err != nil {
					fmt.Println("Could not connect to", CONNECT)
					log.Fatal(err)
				}
				fmt.Fprintf(serverConn, "UNLOCK "+transactionID+" \n")
			}
			// send response back to client
			c.Write([]byte("COMMIT OK\n"))
			return

		case "ABORT":
			go yeetusFetus(transactionID, server)
			// send response back to client
			c.Write([]byte("ABORTED\n"))
			return
		}
	}
}

func handleConnection(c net.Conn, server *Server) {
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		// fmt.Println("Server " + server.branchID + " received message: " + netData)
		request := strings.Split(netData, " ")
		transactionID := request[len(request)-1]
		// fmt.Println("Transaction ID:", transactionID)
		requestType := request[0]
		switch requestType {
		case "BEGIN":
			// if begin, spawn a coordinator
			server.read[transactionID] = []string{}
			go coordinator(c, server)
			c.Write([]byte("OK\n"))
			return

		case "LOCK":
			accountInfo := strings.Split(request[1], ".")
			account := accountInfo[1]
			accounts := server.read[transactionID]
			if server.locks[account].lockHolder != transactionID {
				for idx, readAccount := range accounts {
					if readAccount == account {
						server.locks[account].mutex.RUnlock()
						accounts[idx] = accounts[len(accounts)-1]
						accounts[len(accounts)-1] = ""
						server.read[transactionID] = accounts[:len(accounts)-1]
					}
				}
				server.locks[account].mutex.Lock()
				server.locks[account].write = true
				server.locks[account].lockHolder = transactionID
			}
			c.Write([]byte("UNLOCKED\n"))

		case "UNLOCK":
			readAccounts := server.read[transactionID]
			for _, account := range readAccounts {
				server.locks[account].mutex.RUnlock()
			}

		case "BALANCE":
			accountInfo := strings.Split(request[1], ".")
			// branch := accountInfo[0]
			account := accountInfo[1]
			// fmt.Println("Accessing account ", account, " on branch ", branch)
			if _, keyExists := server.balances[account]; keyExists {
				accountsLock := server.locks[account].mutex
				// fmt.Println("Awaiting lock")
				accountsLock.RLock()
				// fmt.Println("Received lock")
				server.locks[account].read = true
				server.read[transactionID] = append(server.read[transactionID], account)
				// fmt.Println("Server.read["+transactionID+"]: ", server.read[transactionID])
				c.Write([]byte(request[1] + " " + strconv.Itoa(server.balances[account]) + "\n"))
			} else {
				_, lockExists := server.locks[account]
				if lockExists {
					accountsLock := server.locks[account].mutex
					accountsLock.RLock()
					server.locks[account].read = true
					server.read[transactionID] = append(server.read[transactionID], account)
					if _, keyExists := server.balances[account]; keyExists {
						c.Write([]byte(request[1] + " " + strconv.Itoa(server.balances[account]) + "\n"))
					} else {
						c.Write([]byte("ABORTED\n"))
					}
				} else {
					c.Write([]byte("ABORTED\n"))
				}
			}
			return

		case "DEPOSIT":
			accountInfo := strings.Split(request[1], ".")
			// branch := accountInfo[0]
			account := accountInfo[1]
			// fmt.Println("Accessing account", account, "on branch", branch)
			_, lockExists := server.locks[account]
			if !lockExists {
				server.locks[account] = &Lock{&sync.RWMutex{}, "NONE", false, false}
			}
			_, accountExists := server.balances[account]
			if accountExists {
				accountsLock := server.locks[account].mutex
				if server.locks[account].lockHolder != transactionID {
					// fmt.Println("Awaiting lock")
					accountsLock.Lock()
					// fmt.Println("Received lock")
					server.locks[account].write = true
					server.locks[account].lockHolder = transactionID
				}
				c.Write([]byte(request[1] + " " + strconv.Itoa(server.balances[account]) + "\n"))
			} else {
				accountsLock := server.locks[account].mutex
				// fmt.Println("Awaiting lock")
				accountsLock.Lock()
				// fmt.Println("Received lock")
				server.locks[account].lockHolder = transactionID
				server.locks[account].write = true
				val := strconv.Itoa(0)
				_, accountExists := server.balances[account]
				if accountExists {
					val = strconv.Itoa(server.balances[account])
				}
				c.Write([]byte(request[1] + " " + val + "\n"))
				// fmt.Println("not found")
			}
			return

		case "WITHDRAW":
			accountInfo := strings.Split(request[1], ".")
			account := accountInfo[1]
			// fmt.Println("Accessing account ", account, " on branch ", accountInfo[0])
			if _, keyExists := server.balances[account]; keyExists {
				accountsLock := server.locks[account].mutex
				if server.locks[account].lockHolder != transactionID {
					// fmt.Println("Awaiting lock")
					accountsLock.Lock()
					// fmt.Println("Received lock")
					server.locks[account].write = true
					server.locks[account].lockHolder = transactionID
				}
				c.Write([]byte(request[1] + " " + strconv.Itoa(server.balances[account]) + "\n"))
			} else {
				_, lockExists := server.locks[account]
				if lockExists {
					accountsLock := server.locks[account].mutex
					accountsLock.Lock()
					server.locks[account].write = true
					server.locks[account].lockHolder = transactionID
					if _, keyExists := server.balances[account]; keyExists {
						c.Write([]byte(request[1] + " " + strconv.Itoa(server.balances[account]) + "\n"))
					} else {
						c.Write([]byte("ABORTED\n"))
					}
				} else {
					c.Write([]byte("ABORTED\n"))
				}
			}
			return

		case "COMMIT":
			accountInfo := strings.Split(request[1], ".")
			account := accountInfo[1]
			updatedBalance, _ := strconv.Atoi(strings.TrimSpace(request[2]))
			server.balances[account] = updatedBalance
			if server.locks[account].lockHolder == transactionID {
				server.locks[account].lockHolder = "NONE"
				server.locks[account].write = false
				server.locks[account].read = false
				server.locks[account].mutex.Unlock()
			}
			c.Write([]byte("COMMITTED\n"))
			fmt.Println(server.balances)
			return

		case "ABORT":
			// Unlock read locks
			for _, account := range server.read[transactionID] {
				server.locks[account].read = false
				server.locks[account].mutex.RUnlock()
			}
			// Unlock write locks
			for _, lock := range server.locks {
				if lock.lockHolder == transactionID {
					lock.lockHolder = "NONE"
					lock.write = false
					lock.read = false
					lock.mutex.Unlock()
				}
			}
			c.Write([]byte("ABORTED\n"))
			return
		}
	}
}

func main() {
	args := os.Args
	if len(args) != 3 {
		log.Fatal("Please provide branch ID and config file")
	}
	branch := args[1]
	fmt.Println("Branch " + string(branch))

	// Create server
	server := new(Server)
	server.branchID = branch
	branches := make(map[string]string)
	server.branches = branches
	server.balances = make(map[string]int)
	server.locks = make(map[string]*Lock)
	server.read = make(map[string][]string)

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
		currBranch := text[0]
		currAddress := text[1]
		currPort := text[2]
		if currBranch == server.branchID {
			server.port = currPort
		}
		server.branches[currBranch] = currAddress + ":" + currPort
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Start listening
	fmt.Println("Listening on port " + server.port)
	l, err := net.Listen("tcp", ":"+server.port)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// goroutine spawned for every connection
		go handleConnection(c, server)
	}
	fmt.Println("Server branches: ", server.branches)
}
