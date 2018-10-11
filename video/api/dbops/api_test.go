package dbops

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

var tempVid string

func clearTables() {
	dbConn.Exec("truncate users")
	dbConn.Exec("truncate video_info")
	dbConn.Exec("truncate comments")
	dbConn.Exec("truncate sessions")
}

func TestMain(m *testing.M) {
	clearTables()
	m.Run()
	clearTables()
}

func TestUserWorkFlow(t *testing.T) {
	t.Run("Add", testAddUser)
	t.Run("Get", testGetUser)
	t.Run("Del", testDeleteUser)
	t.Run("Reget", testRegetUser)
}

func testAddUser(t *testing.T) {
	err := AddUserCredential("wan9xy", "123")
	if err != nil {
		t.Errorf("Error of AddUser: %v", err)
	}
}

func testGetUser(t *testing.T) {
	pwd, err := GetUserCredential("wan9xy")
	if err != nil || pwd != "123" {
		t.Errorf("Error of GetUser:%v", err)
	}
}

func testDeleteUser(t *testing.T) {
	err := DeleteUserCredential("wan9xy", "123")
	if err != nil {
		t.Errorf("Error of DelUser:%v", err)
	}
}

func testRegetUser(t *testing.T) {
	pwd, err := GetUserCredential("wan9xy")
	if err != nil {
		t.Errorf("Error of RegetUser: %v", err)
	}

	if pwd != "" {
		t.Errorf("Error of DelUser: %v", err)
	}
}

func TestVideoWork(t *testing.T) {
	clearTables()
	t.Run("Insert a user", testAddUser)
	t.Run("Add", testAddVideo)
	t.Run("Get", testGetVideo)
	t.Run("Del", testDeleteVideo)
	t.Run("Reget", testRegetVideo)
}

func testAddVideo(t *testing.T) {
	res, err := AddNewVideoInfo(123, "wan9xy")
	if err != nil {
		t.Errorf("Error of add a video: %s", err)
	}
	tempVid = res.Id
}

func testGetVideo(t *testing.T) {
	_, err := GetVideoInfo(tempVid)
	if err != nil {
		t.Errorf("Error of get a video: %s", err)
	}
}

func testDeleteVideo(t *testing.T) {
	err := DeleteVideo(tempVid)
	if err != nil {
		t.Errorf("Error of delete a video: %s", err)
	}
}

func testRegetVideo(t *testing.T) {
	res, err := GetVideoInfo(tempVid)
	if err != nil || res != nil {
		t.Errorf("Error of reget a video: %s", err)
	}
}

func TestComments(t *testing.T) {
	clearTables()
	t.Run("Add a user", testAddUser)
	t.Run("Add ccomments", testAddComments)
	t.Run("List comments", testListComments)
}

func testAddComments(t *testing.T) {
	vid := "12345"
	aid := 1
	content := "This is a test..."

	err := AddNewComments(vid, aid, content)
	if err != nil {
		t.Errorf("Error of AddComments: %s", err)
	}
}

func testListComments(t *testing.T) {
	vid := "12345"
	from := 1514764800
	to, _ := strconv.Atoi(strconv.FormatInt(time.Now().UnixNano()/1000000000, 10))

	res, err := ListComments(vid, from, to)
	if err != nil {
		t.Errorf(("Error of ListComments: %s"), err)
	}

	for i, ele := range res {
		fmt.Println("comment: ", i, ele)
	}
}
