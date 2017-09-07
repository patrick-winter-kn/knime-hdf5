package org.knime.hdf5;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.core.util.FileUtil;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class Activator implements BundleActivator {

	@Override
	public void start(BundleContext context) throws Exception {
		// get path to lib folder of this plugin
		String newLibPath = getFile("org.knime.hdf5", "lib/").getAbsolutePath();
		// get current lib path and attach it to the front if it is not empty
		String oldLibPath = System.getProperty("java.library.path");
		if (oldLibPath != null && !oldLibPath.isEmpty()) {
			newLibPath = oldLibPath + File.pathSeparator + newLibPath;
		}
		// set new lib path
		System.setProperty("java.library.path", newLibPath);
		// use reflection to force ClassLoader to reload the java.library.path property
		Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
		fieldSysPath.setAccessible(true);
		fieldSysPath.set(null, null);
	}

	@Override
	public void stop(BundleContext context) throws Exception {
	}
	
    public static File getFile(final String symbolicName, final String relativePath) {
        try {
            final Bundle bundle = Platform.getBundle(symbolicName);
            final URL url = FileLocator.find(bundle, new Path(relativePath), null);
            return url != null ? FileUtil.getFileFromURL(FileLocator.toFileURL(url)) : null;
        } catch (final Exception e) {
            return null;
        }
    }

}
