/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 * 
 * History
 *   Oct 10, 2013 (Patrick Winter, KNIME.com AG, Zurich, Switzerland): created
 */
package org.knime.hdf5.nodes.reader;

import org.knime.core.node.ExecutionContext;

/**
 * Class that monitors the progress of a node and updates the progress of the
 * execution context.
 * 
 * @author Patrick Winter, KNIME.com AG, Zurich, Switzerland, KNIME.com, Zurich,
 *         Switzerland
 */
final class ProgressMonitor {

    private final ExecutionContext m_exec;

    private long m_progressed;

    private final long m_total;

    private String m_message;

    /**
     * Create a progress monitor without a total or message.
     * 
     * @param exec The ExecutionContext to notify.
     */
    ProgressMonitor(final ExecutionContext exec) {
        this(-1, null, exec);
    }

    /**
     * Create a progress monitor with a total and without a message.
     * 
     * @param total The target number of units that represent a progress of
     *            100%.
     * @param exec The ExecutionContext to notify.
     */
    ProgressMonitor(final long total, final ExecutionContext exec) {
        this(total, null, exec);
    }

    /**
     * Create a progress monitor with a message and without a total.
     * 
     * @param message The progress message.
     * @param exec The ExecutionContext to notify.
     */
    ProgressMonitor(final String message, final ExecutionContext exec) {
        this(-1, message, exec);
    }

    /**
     * Create a progress monitor with a total and message.
     * 
     * @param total The target number of units that represent a progress of
     *            100%.
     * @param message The progress message.
     * @param exec The ExecutionContext to notify.
     */
    ProgressMonitor(final long total, final String message, final ExecutionContext exec) {
        m_exec = exec;
        m_total = total;
        m_message = message;
        m_progressed = 0;
        update();
    }

    /**
     * Advances the progress towards the total by the given units.
     * 
     * @param units The number of units that the progress has advanced since the
     *            last update.
     */
    void advance(final long units) {
        m_progressed += units;
        update();
    }

    /**
     * Change the progress message.
     * 
     * @param message The new message to display.
     */
    void setMessage(final String message) {
        m_message = message;
        update();
    }

    private void update() {
        if (m_total >= 0 && m_message != null) {
            m_exec.setProgress(m_progressed / ((double)m_total), m_message);
        } else if (m_message != null) {
            m_exec.setProgress(m_message);
        } else if (m_total >= 0) {
            m_exec.setProgress(m_progressed / ((double)m_total));
        }
    }
}
