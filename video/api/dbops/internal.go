package dbops

import (
	"database/sql"
	"go_code/video/api/defs"
	"log"
	"strconv"
	"sync"
)

func InsertSession(sid string, ttl int64, uname string) error {
	ttlstr := strconv.FormatInt(ttl, 10)
	stmtIns, err := dbConn.Prepare("INSERT INTO sessons (session_id, TTL, login_name VALUES (?, ?, ?) ")
	if err != nil {
		return err
	}

	_, err = stmtIns.Exec(sid, ttlstr, uname)
	if err != nil {
		return err
	}

	defer stmtIns.Close()
	return nil
}

func RetrieveSession(sid string) (*defs.SimpleSession, error) {
	ss := &defs.SimpleSession{}
	stmtOut, err := dbConn.Prepare("SELECT TTL, login_name FROM sessions WHERE session_id = ?")
	if err != nil {
		return nil, err
	}

	var ttl string
	var uname string
	err = stmtOut.QueryRow(sid).Scan(&ttl, &uname)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if res, err := strconv.ParseInt(ttl, 10, 64); err == nil {
		ss.TTL = res
		ss.Username = uname
	} else {
		return nil, err
	}

	defer stmtOut.Close()
	return ss, nil
}

func RetrieveAllSessions() (*sync.Map, error) {
	m := &sync.Map{}
	stmt, err := dbConn.Prepare("SELECET * FROM sessions")
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var id, ttlstr, login_name string
		err1 := rows.Scan(&id, &ttlstr, login_name)
		if err1 != nil {
			log.Printf("retrive sessions error: %s", err1)
			break
		}

		ttl, err2 := strconv.ParseInt(ttlstr, 10, 64)
		if err2 == nil {
			ss := &defs.SimpleSession{Username: login_name, TTL: ttl}
			m.Store(id, ss)
			log.Printf(" session id: %s, ttl: %d", id, ss.TTL)
		}
	}

	return m, nil

}

func DeleteSession(sid string) error {
	stmtDel, err := dbConn.Prepare("DELETE FROM sessions WHERE session_id = ?")
	if err != nil {
		return err
	}

	_, err = stmtDel.Exec(sid)
	if err != nil {
		return err
	}

	return nil
}
