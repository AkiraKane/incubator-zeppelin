package org.apache.zeppelin.flink;

import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.Input;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;

import static scala.collection.JavaConversions.asJavaIterable;

/**
 *
 */
public class FlinkZeppelinContext extends HashMap<String, Object> {
  private GUI gui;

  public Object input(String name) {
    return input(name, "");
  }

  public Object input(String name, Object defaultValue) {
    return gui.input(name, defaultValue);
  }

  public Object select(String name, Object defaultValue,
      scala.collection.Iterable<Tuple2<Object, String>> options) {
    int n = options.size();
    Input.ParamOption[] paramOptions = new Input.ParamOption[n];
    Iterator<Tuple2<Object, String>> it = asJavaIterable(options).iterator();

    int i = 0;
    while (it.hasNext()) {
      Tuple2<Object, String> valueAndDisplayValue = it.next();
      paramOptions[i++] = new Input.ParamOption(
        valueAndDisplayValue._1(),
        valueAndDisplayValue._2());
    }

    return gui.select(name, "", paramOptions);
  }

  public void setGui(GUI o) {
    this.gui = o;
  }
}
