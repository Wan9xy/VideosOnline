package dbops

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"go_code/video/api/defs"
	"go_code/video/api/utils"
	"log"
	"time"
)

func AddUserCredential(loginName, pwd string) error {
	stmtIns, err := dbConn.Prepare("INSERT INTO users (login_name, pwd) VALUES (?, ?)")
	if err != nil {
		log.Printf("Add user error: %s", err)
		return err
	}

	_, err = stmtIns.Exec(loginName, pwd)
	if err != nil {
		return err
	}

	defer stmtIns.Close()
	return nil
}

func GetUserCredential(loginName string) (string, error) {
	stmtOut, err := dbConn.Prepare("SELECT pwd FROM users WHERE login_name = ?")
	if err != nil {
		log.Printf("Get user error: %s", err)
		return "", err
	}

	var pwd string

	err = stmtOut.QueryRow(loginName).Scan(&pwd)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}

	defer stmtOut.Close()
	return pwd, nil
}

func DeleteUserCredential(loginName, pwd string) error {
	stmtDel, err := dbConn.Prepare("DELETE FROM users WHERE login_name =? AND pwd =?")
	if err != nil {
		log.Printf("Del user error: %s", err)
		return err
	}

	_, err = stmtDel.Exec(loginName, pwd)
	if err != nil {
		return err
	}

	defer stmtDel.Close()
	return nil
}

func AddNewVideoInfo(aid int, name string) (*defs.VideoInfo, error) {
	vid := utils.NewUUID()
	ctime := time.Now().Format("Jan 02 2016,15:04:05")
	stmtIns, err := dbConn.Prepare("INSERT INTO video_info (id, author_id, name, display_ctime) VALUE (?, ?, ?, ?)")

	if err != nil {
		log.Printf("Add new video error: %s", err)
		return nil, err
	}

	_, err = stmtIns.Exec(vid, aid, name, ctime)
	if err != nil {
		return nil, err
	}

	defer stmtIns.Close()
	res := &defs.VideoInfo{Id: vid, AuthorId: aid, Name: name, DisplayCtime: ctime}
	return res, nil
}

func GetVideoInfo(vid string) (*defs.VideoInfo, error) {
	var (
		aid  int
		name string
		time string
	)

	stmtOut, err := dbConn.Prepare("SELECT author_id, name, display_ctime FROM video_info WHERE id = ?")
	if err != nil {
		log.Printf("Get video info error: %s", err)
		return nil, err
	}

	err = stmtOut.QueryRow(vid).Scan(&aid, &name, &time)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}

	res := &defs.VideoInfo{Id: vid, AuthorId: aid, Name: name, DisplayCtime: time}

	defer stmtOut.Close()
	return res, nil
}

func DeleteVideo(vid string) error {
	stmtDel, err := dbConn.Prepare("DELETE FROM video_info WHERE id = ?")
	if err != nil {
		log.Printf("Delete video info error: %s", err)
		return err
	}

	_, err = stmtDel.Exec(vid)
	if err != nil {
		return err
	}

	defer stmtDel.Close()
	return nil
}

func AddNewComments(vid string, aid int, content string) error {
	cid := utils.NewUUID()
	stmtIns, err := dbConn.Prepare("INSERT INTO comments (id, video_id, author_id, content) VALUE (?, ?, ?, ?)")
	if err != nil {
		log.Printf("Add new comments error: %s", err)
		return err
	}

	_, err = stmtIns.Exec(cid, vid, aid, content)
	if err != nil {
		return err
	}

	defer stmtIns.Close()
	return nil
}

func ListComments(vid string, from int, to int) ([]*defs.Comment, error) {
	stmtOut, err := dbConn.Prepare(`SELECT comments.id, users.login_name, comments.content FROM comments
		INNER JOIN users ON comments.author_id = users.id
		WHERE comments.video_id = ? AND comments.time > FROM_UNIXTIME(?) AND comments.time <= FROM_UNIXTIME(?)`)
	if err != nil {
		log.Printf("List comments error: %s", err)
		return nil, err
	}

	var res []*defs.Comment

	rows, err := stmtOut.Query(vid, from, to)
	if err != nil {
		log.Printf("List comments error: %s", err)
		return res, nil
	}

	for rows.Next() {
		var id, name, content string
		if err := rows.Scan(&id, &name, &content); err != nil {
			return res, err
		}

		c := &defs.Comment{Id: id, VideoId: vid, Author: name, Content: content}
		res = append(res, c)
	}

	defer stmtOut.Close()
	return res, nil

}
