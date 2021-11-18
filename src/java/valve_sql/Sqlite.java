package valve_sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.sqlite.Function;

public class Sqlite {
  public static void createRegexMatchesFunc(Connection conn) throws SQLException {
    Function.create(conn, "regexp_matches", new Function() {
        protected void xFunc() throws SQLException {
          if (args() < 2 || args() > 3) {
            throw new SQLException("number of arguments to regexp_matches must be between 2 and 3.");
          }

          String stringToMatch = value_text(0);
          String regexp = value_text(1);
          // If flags are given, add them to the end of the regex:
          if (args() == 3) {
            regexp = "(?" + value_text(2) + ")" + regexp;
          }

          Pattern p = Pattern.compile(regexp);
          Matcher m = p.matcher(stringToMatch);
          StringBuilder stringToReturn = new StringBuilder();
          if (m.matches()) {
            for (int i = 1; i < (m.groupCount() + 1); i++) {
              if (i > 1) {
                stringToReturn.append("@");
              }
              stringToReturn.append(m.group(i));
            }
          }
          result(stringToReturn.toString());
        }
      });
  }
}
