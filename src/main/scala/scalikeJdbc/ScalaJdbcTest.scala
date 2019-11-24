package scalikeJdbc

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object ScalaJdbcTest {
  def main(args: Array[String]): Unit = {
DBs.setup()
    // 加载配置
    DBs.setup()

    // 更新使用的是autoCommit
    DB.autoCommit (implicit session => {
      // SQL里面是普通的sql语句，后面bind里面是语句中"?"的值，update().apply()是执行语句
      SQL("update tablename set salary=? where id=?").bind(100, 3).update().apply()
    })
    // 插入使用的是localTx
    DB.localTx(implicit session=>{
      SQL("insert into emp (id,name,deg,salary,dept) values(?,?,?,?,?)").bind(1002,"xiaohong","php dey",50000,"TP").update().apply()
    })
    // 删除使用的也是autoCommit
    DB.autoCommit(implicit session=>{
      SQL("delete from dataname where xx=?").bind(3).update().apply()
    })
    // 读取使用的是readOnly
    val a: List[String] = DB.readOnly(implicit session => {
      SQL("select * from xx").map(rs => {
        rs.string("name")
      }).list().apply()
    })






  }
}
